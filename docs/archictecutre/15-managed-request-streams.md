# Managed request streams and safe retirement

Status: P17.11 ownership, acknowledgment, sealing, bounded close and automatic
rotation are implemented (2026-09-09). This document fixes the identity,
ownership and recovery contract. [09](09-implementation-status.md) records
qualification; the throughput and multi-machine gates listed at the end remain
required before P17.11 is closed.

## Identity and compatibility

Legacy `RequestKey`, `AuthenticatedInput`, `MutationReceipt`, Core checkpoints,
raw protocol-one requests, epoch-one operation journals and unqualified 32-digit
operation IDs retain their encodings and retry semantics. Opening another local
store never authorizes advancing that principal's legacy epoch floor.

[Managed model types](../../crates/focal-model/src/managed.rs) introduce an
independent namespace:

| Field | Meaning and constraint |
| --- | --- |
| `cluster`, `ledger`, `principal` | Exact cluster, tenant/session and authenticated actor scope |
| `slot` | Bounded registry slot; zero is valid |
| `generation` | Positive incarnation; registration compares the vacant generation and increments it with checked arithmetic |
| `ordinal` | Positive request position within one stream generation |
| `id` | Independent nonzero request ID, bound together with the ordinal, intent hash and domain/cursor family |

Trusted ingress constructs domain authority from the actual actor and current
policy. A managed key cannot nominate another authenticated principal. The
registration `owner` nonce identifies a durable registration attempt; it is
public CAS identity, not a secret or a second security principal. Processes using
the same authenticated principal intentionally have that principal's authority.
Client ownership safety derives from separate registered streams, shared-store
allocation and exact receipt proofs.

The canonical client operation ID is:

```text
m1:<slot:8 lowercase hex>:<generation:16 lowercase hex>:<ordinal:16 lowercase hex>:<request_id:32 lowercase hex>
```

Cluster/ledger/principal come from the saved `OperationContext`. There are no
whitespace, case, width or version aliases. Parsing never reserves an ordinal or
creates work. A managed ID can resolve only a previously durable reservation.
Within its generation, an ordinal at or below the saved retired prefix returns
`Retired`; a committed later generation also fences earlier generations. An
unknown future ID fails as missing. Neither path runs the expansion closure.
Existing unqualified IDs stay in their bounded, permanent-binding legacy store.
They never silently enter an `m1` namespace.

## Session owner and controls

The existing Session owns one bounded managed registry, alongside its unchanged
legacy receipt tables. Managed domain reduction uses the actual ledger and
principal through a scoped Core entry point. It does not manufacture a principal
or epoch alias and does not insert a legacy receipt and subsequently remove it.
Domain and cursor commands share the same ordinal window. One ordinal can hold
only its exact request ID, intent hash, family and original committed outcome.

The stream revision is a **control-only CAS revision**. Register starts at one;
each new successful ACK or Seal advances it once. Ordinary domain and cursor
receipt insertion does not advance it. Exact control retry returns the saved
receipt without advancing again. Close changes the slot to vacant without
requiring another revision increment, allowing closure at maximum revision.

1. **Register:** persist the intended slot, expected vacant generation, owner
   nonce, window and control request ID before sending. Commit the exact CAS.
   An ambiguous reply retries that registration; it cannot choose a fresh
   generation on its own.
2. **Issue:** enforce the registered ordinal window above its acknowledgment
   floor. Reserve state and result capacity before proposal. Original committed
   outcomes and pending conflicts precede new execution.
3. **Acknowledge:** first fsync each original local receipt. Send a bounded,
   contiguous manifest of complete keys and hashes over all receipt fields.
   Commit the new floor and receipt removal atomically. Both outcome families
   participate; acknowledging a cursor command does not acknowledge its consumer's
   separate delta position or release consumer retention.
4. **Seal:** commit a decision for the exact key, family and intent. Earlier
   committed work returns its original receipt; otherwise a committed seal
   prevents subsequent admission. A timeout, ordinary refusal, abandoned process
   or canceled wait never supplies that fence. Seal is ordered against pending
   domain/cursor work through the same Raft application boundary.
5. **Close:** durably stop local issuance, resolve every ordinal through the saved
   issuance frontier, and retire its results before closing. The server can also
   verify a contiguous sealed tail. The client currently acknowledges that tail
   first. Preserve the closed generation so delayed registration, ordinary work,
   ACKs and seals cannot affect a reused slot. Overflow fails closed.

Controls use completion capacity and identities outside the ordinary window.
A full window must be able to drain. The server retains bounded current stream
metadata and the latest control receipt rather than an unbounded control log in
RAM; older control preconditions fail or reconcile against committed state.
Control results, managed receipts and queries remain separate from legacy epoch
reconciliation's unchanged `Unknown`/`BelowFloor` semantics.

The registry defaults to 64 remembered `(principal, slot)` pairs, 256
ordinals per window and eight MiB per slot, subject to the owner's memory
budget. Closed generations remain in those bounded pairs as vacant fences, so
a principal reuses its own slot above its last generation. Under principal
churn the registry recycles pairs at capacity: a registration that finds no
pair for its principal evicts the vacant pair with the smallest committed
stamp (the longest-closed one), selected identically on every replica from
the same committed rows; an occupied pair is never evicted, and a full
registry of occupied pairs refuses with `Capacity`. The fence an evicted pair
carried survives through the registry's **slot-generation watermark**: the
highest generation it ever assigned, advanced on every registration and
persisted in the `FOCALSS7` checkpoint envelope (an SS6 checkpoint derives it
from its retained pairs). A vacant pair presents `max(last generation,
watermark)` to reads and registrations; a registration cites that presented
generation and is assigned exactly one above it, which is the rule the frozen
client validators hold every reply to. A reassigned or re-created pair can
therefore never reissue a generation another principal's delayed traffic
still names: the evicted principal's old identity is refused as unregistered,
its old receipts read as `Unknown`, and an occupancy read of another
principal's slot reveals neither its owner nonce nor its receipts (the
registry answers only for the authenticated principal's own pairs).

## Durable client ownership

[ManagedOperationStore](../../crates/focal-client/src/managed_store.rs) is one
independently registered stream per private store. All methods run synchronously
on the caller's existing filesystem owner. Each call takes a short filesystem
lock, reloads the authoritative local catalogue, fsyncs changes and releases the
lock before returning. No lock, extra worker or explicit `Arc` survives a call.
Concurrent CLI/MCP processes sharing this store therefore serialize allocation
without holding a lock while waiting on the network.

The durable sequence is:

```text
registration intent → registered receipt
ordinal + independent request ID → normalized intent binding
immutable expanded request + generated IDs → prepared catalogue flag
original receipt → receipt hash catalogue flag
ACK intent → committed ACK receipt + retired prefix → delete old bodies
stopped issuance frontier → exact close intent → committed closed state
```

Only `reserve` creates a managed ID. `outstanding` recovers reservations whose
reply was lost. Preparation binds the operation name, version and canonical
authored bytes before expansion, then publishes the complete request. If
expansion never completed and no request was exposed, that unprepared reservation
can be prepared with the same intent or explicitly sealed. An unsent gap receives
a dedicated persisted seal commitment; the client does not fabricate a business
command to fill it.

Immutable request/receipt publication precedes the catalogue completion flag.
Recovery can adopt the complete checksummed frame after re-establishing its file
and directory durability. Missing records already marked prepared/completed fail
closed. The ACK records a durable retired prefix before removing old request and
receipt bodies. A bounded cleanup cursor resumes deletion after a crash before
another ordinal can issue. Losing a completed body can therefore never make its
old operation ID fresh.

An adapter that automatically creates a store must keep an initialization marker
outside that directory. Missing initialized state is a recovery error, not first
use. A closed store remains a retired namespace. Automatic rotation preserves
the generation fence without accumulating an unbounded directory catalogue: the
coordinator record (`FCLMCO02`) names the active generation's child store, the
store being removed and the last sixteen retired `(slot, generation)` fences;
the server keeps every slot's last generation regardless. No time-based
deletion or process-exit acknowledgment is authorized.

Current client limits are a 32-ordinal default window, configurable up to 256;
one MiB per expanded wire request or receipt; 256 KiB authored intent; a 256 KiB
catalogue; and a conservative aggregate byte reservation covering simultaneous
old/new frames and bounded metadata. Full request and receipt bodies are separate
immutable files, so ordinal allocation rewrites only the bounded catalogue.
The private filesystem layer reuses the legacy store's checksummed frame, mode,
inode/link and atomic publication checks with a distinct managed initialization
marker. Unix private-file semantics are currently required.

### Automatic local ownership and result delivery

[ManagedRequests](../../crates/focal-client/src/managed_requests.rs) now owns the
bounded registration and maintenance state machine used by both adapters. It
probes at most 64 slots in first-fit order. Uncontended first use takes one
authenticated slot read and one exact registration RPC. The slot choice, owner
nonce and request identity are durable before transmission. Concurrent callers
reload after a stale maintenance result; they do not replace an unknown business
request. An outer initialization record and lock remain outside the child store,
so losing initialized child state cannot be mistaken for first use.

The normal human CLI uses `CLI.requests`; MCP uses the independent `MCP.requests`
store. Neither requires users to choose a slot, epoch or generation. Both use the
existing caller-owned filesystem execution seam and release short locks before
network waits. This adapter increment changes private client coordination state,
not the server's managed WAL, checkpoint, request-key or decoder-floor formats.

Ordinary `focal submit` durably reserves an ID internally, binds normalized input
and saves the complete expanded request before sending. Successful output contains
compact object IDs; unresolved operations or failed output receive a copyable
recovery diagnostic. An abrupt process kill remains recoverable through
`request pending`, even if nothing was printed. After a committed result is written and
**flushed**, the CLI durably marks that operation delivered. Maintenance ACKs only
the contiguous prefix of delivered, locally saved results. A later delivered
result cannot reclaim an earlier unobserved one. Flush establishes delivery to
the selected output stream, not consumption by another application. Cleanup
failure after successful output is deferred; it does not turn committed business
success into failure. Normal use therefore exceeds the 32-request default window
without manual acknowledgment.

A timeout, failed output, canceled wait, Refuse or Inform leaves the exact request
recoverable. `request pending` discovers the bounded CLI and MCP stores;
`request inspect --operation-id` does not acknowledge a result; `request retry
--operation-id` resubmits or prints the original outcome. Explicit `request seal`
(alias `request abandon`) resolves a gap to an earlier committed outcome or an
admission fence. It does not cancel a claim or undo committed work. After its
result is flushed, the CLI follows the same delivery/ACK path. A retired operation
returns `Retired` and cannot execute again; its full original result may have been
removed.
Sealing drains an already saved competing control within a bounded retry loop;
it never falls through to submitting the business mutation it was asked to fence.

MCP exposes `request.reserve`, `request.pending`, `request.inspect`,
`request.retry`, `request.acknowledge` and `request.seal`. Reservation is explicitly
non-idempotent and performs no business work: after a lost reservation reply,
discover the original ID rather than reserve and dispatch another. Mutations use
that already reserved ID. Returning, inspecting or retrying a result never marks
it consumed. The caller explicitly acknowledges consumption; acknowledgment then
advances only the contiguous delivered prefix. A seal result likewise requires
explicit acknowledgment. MCP pending discovery is scoped to its own store.
JSON-RPC IDs remain unrelated to durable operation IDs.

Existing unqualified 32-digit MCP IDs, raw protocol-one requests, explicit CLI
`--operation PATH` journals and positional-path inspect/retry remain compatible.
An ID flag is never interpreted as a filesystem path. Managed stores require a
private data directory (`0700`) and files (`0600`). Newly created node directories
satisfy this. Older permissive data directories produce an actionable permission
error; the CLI does not silently chmod them. The [manual guide](../manual-cli.md)
describes owner-controlled migration and the preserved explicit legacy path.

Automatic close and rotation are bounded by ordinal count, never by time. A
generation issues at most its rotation bound of ordinals (65,536 by default;
`FOCAL_MANAGED_ROTATION` sets a smaller bound for campaigns, and the bound is
saved with the coordinator record so a different value cannot open it). Once
the frontier reaches the bound and every ordinal through it is retired, the
store stops issuance durably, the coordinator issues the exact close, and on
the `Closed` reply it records the retired fence, removes the old child store
and observes the slot again before registering above the presented
generation; a crash between those steps resumes them on the next open, and a
lost close or read reply re-issues the same request. References into a
retired generation report `Retired` and never execute again; a reservation
refused during the drain reports `Stopped`. The adapter still does not batch
managed epochs. Separate CLI/MCP windows prevent an unconsumed MCP result from
filling the ordinary CLI window; they do not eliminate the need to resolve
unknown gaps.

## Wire activation and upgrades

[Managed wire contracts](../../crates/focal-wire/src/managed.rs) append new
operations and replies, and require protocol two explicitly. Legacy handshake
structures remain unchanged. QUIC offers versions two and one; Unix protocol-two
requests negotiate before sending business bytes. Protocol-one requests remain
valid on a capable server. A version-two handshake proves syntax support only.

Before proposing the new WAL records, the actual Session must have authenticated
decoder support from every voter in both sets of a joint configuration. Each fact
binds cluster, ledger, group, node, published configuration/index and the actual
format fingerprint. The existing trusted peer transport delivers one verified
peer fact at a time; Actor/Runtime requests cannot install a caller-declared
voter list or activation boolean. Configuration changes invalidate cached facts.
The single-voter case obtains its own support from its compiled decoder.

An installed decoder report alone is insufficient: a peer could restart with an
older binary after its report and before the first managed proposal. Advertising
support therefore requires an irreversible **durable decoder floor** in that
peer's logical WAL. The appended record kind makes older decoders refuse recovery;
every checkpoint retains the floor. On reopening, the consensus owner withholds
Raft output until the actual application confirms its exact compiled decoder.
The floor is established through the existing asynchronous physical writer before
a support fact becomes visible. An ambiguous or pending write provides no support.
Passive fleet inspection leaves untouched legacy groups unchanged; actual managed
admission or an authorized member's support probe establishes demand.

Committed activation preserves that invariant for existing voters: later learner
addition/promotion must independently establish the candidate's durable promise.
An activated group therefore does not require every voter to be reachable again
after a leader restart; ordinary quorum availability remains sufficient for new
work and exact retries. Pending membership still fences incompatible admission.
An existing learner receiving its first managed entry or snapshot establishes
its own floor before passing those bytes to Raft for persistence/delivery.
Snapshot transport must report both acceptance and failure to the originating
physical owner. A rejected floor-pending snapshot, a full outbound queue or a
canceled send cannot leave Raft indefinitely waiting for that transfer. Each
snapshot frame owns a completion sender, while the same owner retains bounded
per-peer flight metadata. Dropping the sender means failure. The owner processes
completion after outstanding persistence, binds it to the current term and
snapshot flight, and ignores obsolete responses. Transport acceptance remains
distinct from snapshot application and quorum commitment. Both ledger and
cluster-control replication require this behavior; feedback cannot depend on
space in an unrelated ingress queue.
The initial floor supports one immutable fingerprint. A later persistent-format
upgrade needs a versioned transition and retained historical decoder support;
arbitrary multi-format upgrades are not implemented by this first activation.

A freshly installed prospective learner reports the actual immutable bootstrap
membership at configuration index zero. The real joining constructor retains
bootstrap voters; an empty configuration is not a valid substitute. A prospective
fact must match that bootstrap membership and the same cluster/ledger/group/node/
format, with the candidate absent from current membership. It can authorize
initial learner admission/history transfer for the nominated candidate. It cannot
stand in for an existing voter's exact current-configuration fact or prove
promotion readiness. Live discovery must follow the bounded pending trusted
membership target, rather than broadcasting every ledger to every enrolled node.
The candidate still authenticates its requester against applied membership. A
fresh candidate whose bootstrap configuration does not include the current leader
needs a separately trusted assignment/membership proof or an authorized sponsor.
General distribution of that proof remains required; an enrolled Node certificate
alone cannot substitute for membership authority.

New domain, cursor and control WAL records have separate magic/version boundaries.
Checkpoint V5 wraps prior checkpoint state and persists the managed registry only
after activation; legacy writers retain their old checkpoint format. Replay
must preserve original managed receipt indexes, floors, generations and cursor
ownership independently of the domain sequence. Mixed-version rejection must
precede introducing a record any voter cannot replay.

## Remaining implementation and qualification

P17.11 remains open until all nine gates in [13](13-cli-and-agent-implementation-plan.md)
pass together. Bounded close, automatic rotation, registry recycling under the
slot-generation watermark and privacy-safe occupancy are implemented and
qualified on one machine ([09](09-implementation-status.md), "Bounded
generations: automatic rotation and registry recycling"). Required remaining
integration includes:

- Managed batch/epoch execution over the existing bounded apply engine. The
  initial single-pending managed entry boundary is a correctness increment;
  throughput and cross-stream fairness still require measurement and refinement.
- Real mixed-version network rejection, authenticated support collection and
  prospective learner lifecycle under membership/owner changes and cancellation.
- Lost controls and ordinary replies, unsent gaps, delayed old-generation traffic,
  concurrent processes and machines, both outcome families, ACK progress at
  capacity, partition/leadership change around seals, and checkpoint-plus-tail
  recovery through the actual client/node paths.

These mechanics must remain automatic on a laptop. Their explicit recovery
surfaces are for diagnosis and resolving unknown work; ordinary claim authors
must not configure epochs, slots, generations, acknowledgment floors or topics.
