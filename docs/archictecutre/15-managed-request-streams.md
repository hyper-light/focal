# Managed request streams and safe retirement

Status: P17.11 implementation in progress, 2026-09-05. This document fixes the
identity, ownership and recovery contract. [09](09-implementation-status.md)
records qualification; the remaining adapter and throughput gates below remain
required before P17.11 is complete.

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

The initial registry defaults to 64 remembered `(principal, slot)` pairs,
256 ordinals per window and eight MiB per slot, subject to the owner's memory
budget. Closed generations remain in those bounded pairs; closing allows that
principal to reuse its slot, rather than freeing its identity fence. Sustained
principal churn and reassignment of finite slots across principals therefore
require a separate globally monotone slot-generation contract and privacy-safe
occupancy discovery before claiming indefinite registry recycling. Increasing a
quota alone does not establish that property.

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
use. A closed store remains a retired namespace; future automatic store rotation
must preserve the generation fence without accumulating an unbounded directory
catalogue. No time-based deletion or process-exit acknowledgment is authorized.

Current client limits are a 32-ordinal default window, configurable up to 256;
one MiB per expanded wire request or receipt; 256 KiB authored intent; a 256 KiB
catalogue; and a conservative aggregate byte reservation covering simultaneous
old/new frames and bounded metadata. Full request and receipt bodies are separate
immutable files, so ordinal allocation rewrites only the bounded catalogue.
The private filesystem layer reuses the legacy store's checksummed frame, mode,
inode/link and atomic publication checks with a distinct managed initialization
marker. Unix private-file semantics are currently required.

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
pass together. Required remaining integration includes:

- Automatic durable registration/store selection for normal CLI and MCP work,
  versioned caller-known reservation/recovery discovery, exact adapter retry,
  and bounded namespace rotation without changing legacy ID semantics.
- Principal-churn qualification and, where slots are reassigned across principals,
  a persistent global slot generation with old-principal fences and occupancy
  responses that cannot expose another principal's receipts or ownership nonce.
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
