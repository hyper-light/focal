# Shared application operation registry

`descriptors()` is the sorted, versioned catalog of 18 implemented authored
operations. `parse_json(name, bytes)` uses the existing bounded strict parser;
`AuthoredOperation::build(context, ids)` returns a `PlannedOperation`. The same
public DTOs can be populated from flags or parsed as YAML through `input`.
Claims, testaments and artifacts reuse their existing builders unchanged.

```text
claim.submit       testament.submit   artifact.submit
claim.post         claim.progress     claim.cancel
receipt.acquire    evidence.begin
claim.get          testament.get      artifact.get       validation.get
claim.list         testament.list     artifact.list      validation.list
request.status     request.epoch
```

The caller-selected durable operation ID, authenticated context and optional
expected revision belong to the adapter. First compare the descriptor name,
version and `canonical_intent()` with the private journal's saved intent. Only
a newly admitted intent calls the fallible ID generator. Persist the fully
expanded command and request identity before transmission. A retry uses the
saved operation; `build` is not a retry mechanism. The canonical authored form
normalizes field order and serde defaults, but deliberately preserves authored
strings and array order rather than assuming those distinctions are irrelevant.

All list filters are optional and conjunctive. `source` means claim issuer;
`target` means subject. `self` resolves only to the selected authenticated
actor. Unsupported family/filter combinations fail. A list cursor is the
server's hexadecimal opaque cursor, preserved exactly as bytes. Limits default
to 64 returned items and 1024 visited rows. Empty pages with continuations are
not exhaustion. A get selects one exact ID; validation get returns bounded
actual run/attempt results. Validation continuation supplies an exact prefix
and run position. These read helpers never collect a whole ledger or choose an
arbitrary first result for a singular filter.

Reconciliation reads bind the complete request key to the authenticated
principal and a fresh owner quorum barrier. Their reply includes the domain
sequence and applied Raft index. Retained domain and cursor results win below
the epoch floor; missing results remain historical uncertainty even when new
admission is fenced. Use `Client::reconcile` to bind the adapter's principal.
These observations cannot authorize replacement intent or journal retirement.

`COMMAND_INVENTORY` and the exhaustive command/query/wire/stream/upload/custody
matches record all 29 model commands and current transport variants. Adding a
variant requires an explicit decision at compilation. `WireAvailable` and
`InternalOnly` entries are not discoverable authored tools. Runtime control
RPCs remain opaque metadata-owner requests, not a Node-to-Runtime permission
conversion. Traversal, durable streams, byte transfer and privileged workflow
or cluster administration still use their existing APIs; this catalog does not
expose wrappers for them. Actor capability metadata is a discovery hint: every
request must still pass authenticated ingress and domain authorization.

Each descriptor supplies local JSON Schema 2020-12 input and output schemas.
They describe strict authored fields/defaults and the common typed
`ApplicationResult` envelope. Tests check DTO field parity, bounded decoding,
canonical command bytes and real Core admission. Further semantic constraints
such as byte budgets, current receipts, pinned requirements and policy revisions
are enforced by the shared builders and server. The output schema deliberately
does not duplicate the entire frozen nested wire model: `focal-wire` validates
those typed payloads. This is not a claim of complete nested JSON Schema
conformance or MCP protocol conformance.

Canceling a wait never cancels business work. `ApplicationResult::is_error`
marks explicit adapter errors, domain refusals and pending mutation outcomes;
Inform/Yield remain domain conditions. Stored failed validation verdicts are
successful reads of failed validation evidence, not adapter execution errors.
