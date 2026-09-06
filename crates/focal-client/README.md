# Focal client

The client preserves typed request identities across transport retries. Its human
input module is shared by CLI flag builders and JSON/YAML documents; it does not
open a ledger, grant authority, or send a command while parsing or building it.

## Human input

`input::parse_document::<T>(bytes, InputFormat::{Json,Yaml})` decodes the strict
public document types. `ClaimDocument`, `TestamentDocument`, and `ArtifactDocument`
provide `build(self, &BuildContext, &mut impl IdGenerator) -> Result<Command,
InputError>`. The caller supplies the selected ledger, authenticated actor, actual
root cause, and pinned policy revision. Ingress independently verifies that
context; constructing it in a client grants no permission.

IDs are exactly 32 hexadecimal digits and hashes are exactly 64. Zero, whitespace,
separators, malformed digits and wrong widths are rejected. Uppercase hex produces
the same typed value. The participant fields `target`, validation `evaluator`, and
`contributed_by` also accept `self`, resolving only to `BuildContext.actor`.
Other names are rejected. Public `parse_id`, `parse_hash`, `resolve_participant`, and `parse_*` vocabulary
helpers are available to flag and query adapters. Vocabulary names use snake case,
such as `whole_work`, `test_surface`, and `testament_generated`, while the model's
existing numeric wire encodings remain unchanged.

A valid minimal claim document explicitly defines its delivery validation:

```json
{
  "description": "Respond with the report",
  "target": "00000000000000000000000000000006",
  "validations": [{
    "kind": "receipt",
    "phase": "whole_work",
    "mode": "required",
    "description": "Receive the closing testament",
    "evaluator": "00000000000000000000000000000005"
  }]
}
```

This delivery-only specification establishes no quality check. Applications that
require tests or review must provide those validation definitions, including exact
handler IDs/version hashes and accepted evidence schemas. A quality bar requires
a programmatic handler before a single final agentic handler. The builder derives
requirement hashes from the complete supplied definitions. It never inserts a
passing validation or substitutes an available handler for a missing version.

Claim IDs, occurrence IDs, validation IDs, testament IDs and artifact IDs can be
provided or allocated through the fallible injected `IdGenerator`. Before sending,
the caller must journal the resulting expanded command and complete transport
envelope. Retrying an unknown outcome reuses that record; it must not rebuild from
the original document and generate new IDs. The default claim action is `work`;
self-targeting requires an explicit legal `handoff` action.

Artifact payloads support `{"type":"text","text":"..."}`,
`{"type":"inline","bytes":[...]}`, or `{"type":"content","reference":...}`.
The content-reference fields are `domain`, `root`, `length`, and `class`.
Attachment also requires a claim, receipt fence and evidence-set ID. Producer and
artifact receipt come from the supplied context/fence; client input cannot assert
durable custody or successful schema validation. Those are independently enforced
by the server. A closing testament supplies the exact ordered artifact ID/hash
manifest; it does not discover or silently attach other evidence.

Decoding admits at most 256 KiB, depth 16 and 8,192 value nodes. JSON receives a
bounded shape/duplicate-key pass before typed decoding; YAML uses parser budgets,
duplicate-key rejection and disabled aliases, anchors and merge keys. Unknown
fields, multiple documents, trailing values and unsupported YAML tags fail.
Builders also enforce collection/text/inline limits for directly constructed DTOs.
Empty objects parse as documents but fail required-field validation. Deadline
values are explicit logical timestamps and timer fences; this layer does not
translate wall-clock time or mint server clock authority.

The input tests check flags/JSON/YAML command and canonical identity parity,
malformed inputs, generator failures, pinned validation contracts, actual core
claim/testament admission, and refusal of artifact attachment without trusted
custody. They do not claim to implement the complete planned shell command matrix.

## Durable manual operations

`pending::OperationJournal::create(path, context, open_epoch, request)` saves both
complete envelopes, including every generated ID, before either can be sent. The
path names a new private operation directory; its parent must already exist.
`OperationJournal::open(path, &context)` resumes only the saved cluster, principal
and ledger. It never recreates a missing directory or missing state. Retry from
that path rather than rebuilding the authored document.

Hold the journal owner while sending its borrowed `next_request()`. Pass a
committed or duplicate `MutationReply` to `record_reply()` before sending the next
request. The stages are `OpenEpoch`, `Command`, and `Completed`; `receipt()` exposes
the final durable result. All manual operations use admitted epoch one and never
advance an epoch floor, so another CLI or MCP operation cannot retire an unknown
request. The server independently authenticates each transmission.

The journal uses an exclusive filesystem lock, directory mode `0700`, file mode
`0600`, a checksummed bounded record, atomic rename, and file/directory fsync. Each
request and receipt may occupy at most one MiB of serialized data; larger
capabilities return `PendingError::Capacity`. Unix private-file semantics are
required. Existing nonprivate files, symlinks, hardlinks, invalid state, and changed
receipt identities fail closed. A complete record interrupted before creation of
its initialized marker can be recovered; a missing record cannot.

Run these synchronous journal methods on the CLI's OS main thread outside
`Runtime::block_on`, with network waits between journal calls. Cancellation or an
unknown/refused remote result leaves the exact request pending. An ambiguous local
write disables further requests on that owner until it is dropped and reopened;
recovery accepts the complete old or new generation. The journal contains authored
payloads and should remain private. Its error messages do not print those payloads.

Tests execute the saved requests through the actual core, deliberately lose both
epoch and business replies, cancel a client wait after commitment, inject failures
at durability boundaries, and verify concurrent fixed-epoch operations, exact
generated identities, private locks, corruption, missing state, and capacity.

## Managed request streams

The new `managed_store::ManagedOperationStore` adds a separate private stream
allocator for independently fenced request histories. It persists registration,
ordinal reservations, normalized intent, expanded envelopes, receipts and exact
ACK/seal/close controls. Methods release their filesystem lock before network
waits; `Client::submit_managed`, `request_stream_control` and
`request_stream_read` only perform authenticated transport work.

Managed operation IDs use the explicit fixed-width lowercase
`m1:slot:generation:ordinal:request_id` namespace. `reserve` creates them;
`prepare` cannot create a missing reservation. Compaction records a durable
retired prefix before deleting old bodies, and old IDs return `Retired` without
running an expansion closure. Existing 32-digit operation IDs and epoch-one
journals retain their current semantics. Automatic CLI/MCP managed selection
and namespace rotation remain integration work. The detailed API lifecycle,
limits and upgrade gates are in the
[managed-stream architecture](../../docs/archictecutre/15-managed-request-streams.md).

## Reconciliation reads

`request.epoch` accepts `{ "epoch": 1 }`; `request.status` accepts an epoch and a
32-digit hexadecimal `request_id`. Their shared strict DTOs build read-only
`Operation::Reconcile` queries without generating IDs or advancing epochs. The
server derives the principal from authentication, obtains an authoritative read
barrier, and reports the exact published prefix. Actor and Runtime credentials
can reconcile their own requests; Node and Evaluator roles cannot use this path.

Use `Client::reconcile(request, expected_principal)` when the adapter knows its
authenticated identity. It checks that identity after transport validation has
bound the query, target request key, ledger, route and published prefix. Replies
also carry the applied Raft index, which orders cursor metadata independently of
the domain sequence. The
structured result is `OperationOutput::Reconcile { reply }`. A retained committed
receipt remains resolvable even below the epoch floor. `BelowFloor` means new
admission is fenced but history may be unavailable; `Unknown` means no retained
receipt is visible. Neither is evidence that the original request never committed
or permission to generate a replacement intent. Inspection leaves the durable
operation journal unchanged.

`CommittedCursor` preserves the complete original cursor mutation receipt,
including its metadata revision, Raft index, replay floor, optional cursor record,
filter and mode. It is distinct from a domain `Committed` receipt; an adapter
reconciling a saved domain mutation must reject a cursor receipt as a key-family
mismatch instead of treating it as the domain operation's success.

## Shared operation IDs

`operation_store::OperationStore` maps a caller-known, nonzero 32-digit hexadecimal
operation ID to one exact journal. Explicit `create(root, limits)` initializes a
new private store; `open(root, limits)` requires the existing initialized catalogue.
A host providing automatic first use must keep its initialization marker outside
the store directory, so loss of the whole store cannot be mistaken for first use.

`open_or_create(id, context, intent, expand)` compares the saved cluster, principal,
ledger, operation name/version and canonical authored intent before calling the
expansion closure. Use the shared operation registry's normalized intent, including
any separately authored optimistic revision. Different intent under the same ID
fails. Existing operations reuse complete saved envelopes without generating IDs.
`open_existing(id, &context)` inspects or retries using the saved intent; an unknown
ID cannot create an operation through this method. `operation_path(id)` exposes the
claimed journal path for the existing explicit path-based inspect/retry interface.

Admission first fsyncs the ID claim, then saves immutable prepared intent and both
expanded requests, creates the journal, and finally fsyncs catalogue readiness.
Nothing is returned for transmission until all those steps complete. A crash after
complete preparation can finish journal creation from those same requests. A claim
without complete prepared requests remains `StoreError::Incomplete`; retry never
reruns expansion. Missing ready journals, catalogues or store markers fail closed.
If a crash leaves the known temporary and destination as the only two links to
one private inode, recovery verifies the complete bounded frame and checksum,
removes only that temporary link, and fsyncs the directory. A different inode,
third link, symlink or damaged frame remains an error. This applies to the initial
catalogue and immutable prepared records without changing their stored bytes.

An unready catalogue entry with complete prepared requests may also resume initial
journal creation after directory/lock creation or an unfinished initial temporary.
That narrow recovery requires absent `state.bin` and `INITIALIZED`, and only the
expected private lock/temporary remnants. It validates the prepared requests
before repair. Existing valid state must match the exact requests and retains all
receipts; corrupt state or an initialized journal missing its state is never reset.
The catalogue lock covers registration only; each returned journal independently
holds its own operation lock across network waits. Store methods are synchronous
and belong on the same blocking owner as journal persistence.

Intent is limited to 256 KiB. `StoreLimits` records both an operation-count cap and
an aggregate byte reservation. Every claimed ID reserves the worst case for its
bounded journal generations and prepared files, including incomplete or completed
operations; there is no automatic eviction. Defaults permit 256 claimed IDs and
512 MiB of reservations, with whichever bound is reached first controlling
admission. This is a conservative bound on store-owned record/temporary bytes,
with a small metadata allowance; it does not promise a filesystem-specific bound
on allocated blocks. Changing saved limits requires an explicit future migration.
