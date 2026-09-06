# Shared Focal workflow contract

These repository skills are instructions over the implemented local actor tools. Their manifest pins instruction content and operation versions; it grants no capability. Install or copy the whole `skills` directory so the relative references remain valid. Current MCP composition uses the laptop/founder's local principal and ledger. Joined-node and remote authenticated client contexts are unavailable through this adapter.

## Before a mutation

Choose a caller-known, nonzero **32-character lowercase hexadecimal** `operation_id` and preserve it with the intended operation. The JSON-RPC request ID only correlates a transport call. Each separate mutation—submit, post, acquire, begin, attach, close—needs its own operation ID.

The adapter durably binds the operation ID to the cluster, principal, ledger, operation name/version, normalized authored input and optional optimistic revision. It saves every generated ID and both the epoch-open and business envelopes before transmission. Repeating an ID with changed intent returns an operation conflict. Retry uses the exact saved identity, including after a process restart; newly authored work gets a new ID.

## Interpret the result

Use MCP `structuredContent`, whose application envelope contains `schema_version`, `operation_id`, `condition` and `result`. A verified committed mutation receipt proves that particular ledger command. Read the actual claim lifecycle and requirements to establish satisfaction. Progress, artifact admission, testament generation, a receipt-only validation and the author's `confidence` are separate facts.

On an unknown outcome, cancellation or lost reply, preserve the operation ID. `request.inspect` reads the saved state; `request.retry` transmits only the saved pending request(s). Both take `operation_id`, and neither creates an unknown ID. Domain refusal remains a reported outcome with saved state. An incomplete/corrupt store fails closed: report the specific error and retained ID rather than automatically replacing it or deleting recovery files.

`request.inspect` also accepts optional `remote: true`. It queries the owner's retained business receipt at a fresh quorum barrier, verifies the saved command hash and any existing receipt, and leaves the journal unchanged. It queries the business key even if epoch admission is the locally pending step. The returned result describes the owner's observed prefix; absence never erases a locally saved receipt. `request.status` accepts `epoch` and `request_id` without a local journal; `request.epoch` accepts `epoch`. The principal comes from authentication. A retained receipt means committed. `BelowFloor` means that new admission is fenced while historical commitment remains unknown. `Unknown` means an earlier proposal can still commit. Neither result authorizes regenerating an uncertain command, deleting its journal or advancing another process's epoch floor. Quorum loss produces a retryable operational error, not a successful absent-receipt observation.

MCP cancellation ends client interest and can suppress the reply. It cannot retract an admitted write or cancel a claim. `claim.cancel` is the explicit, separately journaled business operation. This release has no durable parked-agent continuation or autonomous wake-up loop.

## Preserve read scope

List filters are optional and conjunctive. Preserve the same filters across continuations. List results contain `page.next.bytes`; encode those exact bytes as hexadecimal for the next tool's `cursor`. An empty page may still have a continuation. Treat list cursors as opaque scope/prefix proofs; an expired view requires a new read, with the prefix change made explicit.

`validation.get` returns its requirement plus bounded run summaries and verdict records. For another page, use the returned exact read token's sequence/route epoch as `prefix`. In the `ValidationResults` object's `next` position, map `run.target_hash`, `run.phase`, `run.epoch` and `attempt` to the flat `after` fields in the discovered schema; retain the original validation `id`. Nested results use the frozen wire representation: IDs/hashes are byte arrays and vocabulary values are numeric. Convert bytes to hexadecimal exactly. Version-1 validation phases are `1` → `admission`, `2` → `increment`, `3` → `whole_work`. A requirement with no runs is known but unexecuted.

## Consultations and challenges

Use the original claim, disputed evidence and pinned requirements as the work contract. A **challenge** obliges the responding agent to deliver artifacts proving the challenger's stated claims. Identify each assertion and the artifact/check that establishes it. A response label, receipt, unsupported defense or empty proof cannot resolve the obligation.

When required challenge proof fails, the workflow calls for corrective claims. Preserve the evidence and identify the concrete correction, responsible subject and acceptance checks. Where the current authenticated actor and available ordinary claim operations can author that work, create an explicit correction claim and retain its IDs. Otherwise report the outstanding corrective obligation. Automated remediation ownership, duplicate-event handling, parented peer admission and policy-enforced issuance are not implemented; never report a correction as created until its actual receipt exists.

A **consultation** requests satisfactory work under its stated quality bar. Complete that work with artifacts and a testament. Additional clarification or residual work normally becomes an explicit follow-up consultation; a distinct discovered defect can warrant correction. Keep references to the prior work and preserve its immutable history. The current tools expose the `consultation`, `challenge` and `correction` action vocabulary through ordinary claim submission; they do not supply a peer resolver or automatic exchange policy.

Quality checks require real installed handlers and assigned evaluators. Missing evidence, a negative verdict and failed evaluation machinery remain distinct. The MCP actor surface exposes requirement/result reads, not evaluator verdict writes, runtime acknowledgment/completion, membership, placement or enrollment administration.
