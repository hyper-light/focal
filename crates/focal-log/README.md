# Physical WAL ownership

`SharedWal::open` starts one bounded disk-owner thread. The existing
`WalLease::append` interface submits an encoded, owned batch and blocks until its
covering segment data and `CURRENT` fence are durable. The consensus adapter also
uses asynchronous tickets so one shared owner can queue Ready writes for several
groups before any disk wait. No second log or altered Raft acknowledgment rule
is introduced. Requests already queued when the writer wakes share a flush;
there is no batching timer.

An asynchronous host can call `WalLease::append_async(&records)` and await the
returned `WalAppend`, poll `try_complete()`, or use `wait_blocking()` from a
synchronous owner. The method encodes input and
reserves frame, recovery-index and queue capacity before returning. The caller
may release its original records immediately. Dropping a ticket cancels interest
in its result; accepted writes still execute. A ticket retains its small response
allowance until it is consumed or dropped and does not retain the writer handle.
After a receipt is consumed, another poll or a subsequent await returns
`ReceiptConsumed`; it never polls Tokio's already-consumed receiver.
An owner must await a Ready's durable receipt before releasing its Raft messages
or advancing persistence state. `DurableNode::try_drain` retains that ownership
across polls, including the additional LightReady hard-state fence. Its blocking
`drain` compatibility path waits the same exact ticket. A bounded one-slot signal
supports synchronous waiting even inside a Tokio runtime; the future's receipt
and that signal share the existing ticket reservation and hold no writer handle.

Defaults admit 64 data requests plus four reserved control slots, at most 64
requests per flush, and 65,536 logical groups. `WalOptions::max_batch_bytes`
bounds the entire physical batch. Small hard-state/configuration/identity writes
can use the reserved slots and completion memory; large entry/checkpoint batches
use ordinary admission by default. Trusted consensus owners use `append_in`,
`append_async_in`, and `rewrite_checkpoint_in` with the completion lane so
already-admitted work can finish under ordinary pressure. `open_with_budget`
connects these internal limits to a node's shared or
hierarchical `MemoryBudget`. RAM pressure rejects before enqueue. Index outer
container allocation failure after admission stops the physical writer rather
than skipping a group's earlier entry and persisting a later hard state.

Checkpoint, fault, replay and lease commands delimit batches. Records retain
queue order within each logical group; all receipts in a physical batch become
eligible after the same covering fence. An ambiguous write/flush/fence failure
fails every affected receipt and stops subsequent admission to that physical
stream until reopening. The disk format and its crash recovery fence are
unchanged. The final shared handle closes admission, drains accepted commands,
and joins the writer before releasing the directory lock. Its single `Arc`
exists only for this shared lifecycle across independent group owners; disk
state has no mutex.

`SharedWal::writer_id` exposes an opaque, process-local `WalWriterId` minted by the existing checked `OwnerId` counter. Managed owners use it to admit sessions only from a pre-retained physical writer set. It is neither a disk identity nor a serialized authority, and it introduces no shared allocation.

The disk thread has an explicit 2MiB stack reservation. Checked container-size
calculations cover the bounded ingress queue and concurrent batching metadata.

Startup builds a bounded in-memory index of frame locations during the existing
full durable-prefix validation scan. Per-group replay seeks only that group's
frames and rechecks their framing, checksum and identity. It does not scan all
other groups again. A one-record channel bounds recovery delivery. A callback
guard rejects synchronous WAL reentry and polling an unfinished async receipt
with a typed error, preventing the visitor from waiting on its blocked writer.
An async append may enqueue without waiting; cancellation still preserves that
admitted write. Callbacks must return. The caller owns the
budget for any records retained after its callback. Compaction builds the next
index while streaming the replacement generation before installing its fence,
retaining the old index until success and preserving active empty groups.
`WalWriterStats` exposes scan/replay record
counts and physical group-commit counts for verification.

Tests cover coalesced flushes, batch/queue bounds, completion reserve, cancellation,
lease reuse, final-handle shutdown with outstanding tickets, replay isolation,
compaction, rollback on quota rejection, and all existing injected durability
failure boundaries. These checks do not qualify drive firmware behavior or
establish multi-region throughput.
