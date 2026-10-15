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
across polls. Its blocking
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
stream until reopening. The final shared handle closes admission, drains accepted commands,
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
budget for any records retained after its callback.
`WalWriterStats` exposes scan/replay record
counts and physical group-commit counts for verification.

## Checkpoints and reclamation

A group's checkpoint (`rewrite_checkpoint*`) is one group commit: the records
the group keeps, written at the tail, and a floor frame (`RecordKind::Floor`)
that retires every frame the group held before. No other group's frame is read
or written for it, and the volume is asked for those records and the floor
alone.

The `CURRENT` fence (version 2; a version 1 fence is read as a prefix that
starts at the stream's first frame) names the tail and the base: where the
durable prefix starts. The writer moves the base toward the tail. A segment
that holds nothing live is left without being read. While the log holds more
dead bytes than live ones and a segment — the point past which a whole pass,
which writes the live bytes once, frees more than it writes — the frames the
base meets are read and verified, and each live one is written again at the
tail as `RecordKind::Moved`, carrying its origin: the sequence it was first
written at. A group's order is its origins' order wherever its frames stand,
and a floor is compared with origins, so a frame may be moved alone and any
number of times. The frames written again and the new base become durable by
one fence; the segments behind the base are removed after it, and again at the
next open if a crash came between.

Cleaning is bounded and paid for. Inside a commit it may write again no more
bytes than the callers' own commands wrote (banked up to one step), behind
their fence and their flush. While no command waits the writer takes one step
at a time — at most a batch read and a batch written again — and looks at its
queue between steps. A step that finds no memory or no room on the volume
leaves the base where it is.

A reopen reads each frame's header first: a floor sets its group's floor and
the count of the frames it kept that the scan still reads (those the base
passed were written again after the floor and are counted there). The second
reading places each frame whose origin is at or above its group's floor, and
each group's frames are put in the order of their origins. `Wal::replay`, the
single owner's reading in physical order, delivers the live records of a
stream with floors and refuses one that holds moved frames
(`LogError::Relocated`). A caller's batch that holds a floor or a moved frame
is refused (`LogError::Identity`): they are the physical layer's own.

`WalWriterStats` reports the bytes of every frame from the base to the tail
and of the live ones, what checkpoints wrote, what the base passed, the
segments removed, and the frames written again. `benches/checkpoints.rs`
measures the whole against a log of many cold groups and a few hot ones.

Tests cover coalesced flushes, batch/queue bounds, completion reserve, cancellation,
lease reuse, final-handle shutdown with outstanding tickets, replay isolation,
checkpoints and cleaning (a checkpoint's own bytes on a constrained volume, a
cold group met by the base lap after lap, a base inside a checkpoint's frames,
cleaning while idle, a version 1 fence, seeded histories with cuts at every
durability boundary), rollback on quota rejection, and all injected durability
failure boundaries. These checks do not qualify drive firmware behavior or
establish multi-region throughput.

`RecordKind::DecoderFloor` appends enum ordinal 6 without changing older record
encodings. Consensus uses it for an immutable group decoder fingerprint, persisted
before advertising support and retained by every checkpoint. Older record decoders
reject this new kind during the physical WAL validation scan, preventing a
downgraded process from participating even before new application commands exist.
The framing, checksum and durability fence remain unchanged. The WAL does not
interpret application fingerprints; the consensus/application boundary verifies
the owning group's exact requirement before releasing Raft output.
