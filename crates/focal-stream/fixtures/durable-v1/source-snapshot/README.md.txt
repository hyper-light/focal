# focal-stream

Durable cursor state and bounded, resumable delivery of immutable ledger deltas.
There is no durable outbox or second event store in this crate. A `DeltaSource`
replays the authoritative retained ledger/log/archive at a published prefix.

`Position` binds a ledger and represents either an exact `(sequence, ordinal)`
delta or `Resolved(sequence)`. This distinction handles partial transactions,
multiple deltas in one transaction, and transactions with no deltas. A position
within sequence S retains the complete transaction S. Consumer tokens also bind
consumer identity, generation, and the authenticated query/authority scope hash.
These values are protocol data, not cryptographic credentials; ingress performs
authentication and scope authorization before using them.

`CursorRegistry` owns serializable per-consumer state, explicit lease expiry,
snapshot-seed phase, and the retained-history floor. Its preparation builds and
accounts a candidate before any durability decision. The embedding ledger must
persist `CursorCommand` in its authoritative log, then call `publish`, before
reporting an acknowledgment. `replay_committed` reconstructs state from that log;
`CursorCheckpoint` can be included in a verified durable checkpoint. None of
these pure operations performs IO or makes unpersisted state durable.

Registration or `BeginSeed` pins the replay suffix immediately. `CompleteSeed`
opens live delivery only after the consumer durably installed the snapshot.
Seed reset increments the generation, so stale acknowledgments cannot advance
the new stream. Installing a seed replaces/merges projection state; it does not
re-execute irreversible external effects. Expired or explicitly reset projection consumers
stop pinning retention and receive typed Resync on reconnect. Trusted service
consumers can instead use `CursorMode::Protected`: their obligations never expire,
cannot be reseeded or resynced, and pin retention until an explicit durable ACK.
`RegisterProtected` is a server-owned capability, never a generic client option.
`AcknowledgeAndRenew` atomically advances and extends a projection lease; an
invalid ACK leaves both position and expiry unchanged.

`Subscription` is a separate, disposable transport queue. The source visits
every historical delta through its declared coverage; stream filtering happens
after the visit. `pump` enforces item/byte/sequence-work bounds and stages its
result atomically. Invalid IDs, ordinal gaps, cross-ledger output, source errors,
and allocation failures leave queued state and the replay position unchanged.
The source must certify that skipped sequence numbers had no deltas and that a
returned Resolved prefix is complete; the stream cannot independently prove
what a dishonest or broken source omitted.

Data delivery consumes both item and byte credits. Queue fullness backpressures
replay and never moves a durable cursor. A separate reserved control allocation
keeps Resync deliverable with full data buffers and zero credits. Exactly one
control delivery can be in flight: a held delivery backpressures the next dequeue
without consuming a pending Resolved or Resync. Data deliveries own their permits;
only the reserved control permit is shared between the owner and its detached
transport delivery. Resolved waits
behind all staged matching data. A projection consumer exceeding its configured lag,
expired lease, or cursor below retained history receives explicit Resync.
Protected consumers ignore lease/lag expiry; loss of their required history is
a source violation, and never authorizes skipping an effect.

Runtime integration:

1. Authenticate and bind a consumer/filter/scope, prepare its registry command,
   durably commit it, then publish it.
2. Resume `Subscription` from that durable `CursorRecord`; grant transport
   credits and call `pump` using a bounded authoritative source adapter.
3. Write `Delivery.event()` messages in dequeue order. Transport completion
   alone never advances the acknowledged cursor.
4. The consumer prepares its local `ConsumerCheckpoint`, persists the effect
   and checkpoint atomically, then acknowledges the exact token. Duplicate
   delivery after a lost acknowledgment performs no repeated effect.
5. Validate acknowledgments using `acknowledge_command`, durably commit and
   publish them through the registry, then call `acknowledged_committed`.
6. Run `reconcile`/`pump` on a bounded runtime schedule even without hints. This
   recovers the final lost notification. Persist terminal Resync when abandoning
   a consumer pin; disconnecting a transport alone never deletes its cursor.
7. Advance physical log/archive retention only after the corresponding checked
   `AdvanceFloor` command is durable. Respect other checkpoint/replica/restore
   obligations in the embedding retention coordinator as well.

The node adapter supports both single-voter and replicated session owners.
`Streams::begin` retains a bounded owned request and issues a unique ReadIndex
context. The host forwards every Raft message and passes each poll result to
`Streams::advance`; only the matching current-term barrier permits cursor
proposal, and only its exact durable `CursorReceipt` permits a reply. Abandoning
a waiter releases its staging memory without canceling or acknowledging an
admitted cursor command. A retry uses the original principal/epoch/request ID and
intent hash, and durable progress never moves backward. Leadership changes before
proposal return unavailable; after admission they return outcome unknown until
the client retries against current authority.

Seed capture uses the completed read barrier, then commits the seed's suffix pin
before returning its first page. Node seed pages are limited to 64 KiB and hold a
conservative graph-object memory allowance before cloning. Further pages retain
the captured prefix; completing the seed durably opens delivery strictly after
that prefix. If failover loses the original in-memory seed lease after the graph
has advanced, an exact retry reports `SnapshotExpired`, requiring an explicit
new seed generation. It never silently substitutes a newer snapshot. Network
projection commands cannot renew, reset or release protected consumer pins.

The registry currently clones a bounded consumer partition during preparation;
partition sizes and memory allowances must cover that transient copy. Candidate
state is owned and publication validates a process-local owner identity plus
revision, with no reference-counted metadata roots. Its
consumer count is explicitly bounded, and automatic cursor garbage collection
is not implemented. `focal-ledger::Session` now persists this registry through its own Raft log,
retains the required bounded delta tail and checkpoints both together. The runtime
owns network transport, timer scheduling, authentication, and snapshot byte
transfer; durable archive and consumer/outcome reclamation remain future work.
Tests here cover deterministic cursor-log/checkpoint recovery and delivery state
machines; they are not multi-process crash qualification of those adapters.

```sh
cargo test -p focal-stream --offline
cargo clippy -p focal-stream --all-targets --offline -- -D warnings
```
