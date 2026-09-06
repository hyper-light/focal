# Shared Focal workflow contract

These repository skills are instructions over the implemented authenticated participant tools. Their manifest pins instruction content and operation versions; it grants no capability. Install or copy the whole `skills` directory so the relative references remain valid. The selected named client context determines the authenticated principal and ledger; the local context uses the service’s established identity. Switching contexts never changes the authority of a saved operation. Operator tools, when advertised, retain their separate local administrative standing.

## Peer execution and distinct objects

The issuer or designated evaluator invokes validation tools, skills, scripts or native code in its own environment. Public contracts are language agnostic; JSON/YAML describes authored input, not a required execution framework. A peer may request another participant's help through ordinary claims and testaments. Focal records authorized facts and verifies their permitted consequences; it does not launch agents, lease jobs or execute supplied tools. Corrective and follow-up claims are authored by an authorized participant with durable request identities.

Keep the four object families distinct: a claim states an obligation, an artifact carries independently visible evidence, a testament closes an exact response manifest, and a validation declares a check with separate run/verdict evidence. Custody is not claimant receipt, manifest inclusion is not a passing check, and testament generation is not claim satisfaction. Public peer evaluation operations use the current single-response reducer. Independent artifact/testament lifecycle histories remain incomplete. Report only facts actually recorded; never infer missing per-artifact receipt or evaluation events from the claim's status.

## Report completed or unsuccessful work

The current receipt holder authors the response testament when its work completes
or fails. Acquiring the execution receipt creates no testimony. Record a factual
summary and explicit `complete`, `partial`, `refused`, `impossible`, `interrupted`
or `failed` outcome. Every non-`complete` account requires at least one durable
`kind: "error"` artifact produced by that holder under the same claim, current
receipt and evidence set. Partial outputs may accompany it. Close the exact full
ordered manifest only after every referenced attachment is committed; the current
storage model permits one closing testament per claim.

Use real diagnostic evidence when no work result was produced. A tool that never
ran supplies no test counts: use the built-in error-report below instead. For
tests that actually ran and failed, the existing test-report schema with truthful
counts can also be submitted with `kind: "error"`. Artifact kind, respondent
outcome and evaluator verdict are separate fields with separate purposes.

The requester acknowledges the exact response with `testament.receive`; this
neither creates the response nor establishes quality. The designated evaluator
retrieves its bound work and error artifacts, performs the declared checks, and
registers its own proof before `validation.submit`. An unsuccessful account does
not bypass receipt or evaluation, and its outcome cannot stand in for a verdict.
Preserve the original evidence and operation ID if a submission remains unknown
or is refused; report the remaining reporting obligation rather than inventing a
committed response.

### Built-in error-report v1

Schema hash: `add7b85a5a6fac7b753f85551b38c7ef913e55cd8c896c63bb00c6db19b00ab4`.
This reviewed payload contract is pinned here for MCP-only participants. When the
CLI is available, `focal schema get error-report` returns the descriptor, exact
hash, limit and example. `focal schema get test-report` describes actual test
counts. MCP currently exposes neither payload-schema resource discovery nor a
schema registration tool; `tools/list` describes the authored operation inputs.

An illustrative diagnostic payload is:

```json
{"code":"tool_unavailable","message":"The required tool could not run","details":"No test result was produced"}
```

Replace these statements with the actual incident. Supply this JSON as the text
payload of `artifact.submit`, with the pinned `schema_hash`, `kind: "error"`, and
the current claim/receipt/evidence set. Retain the committed artifact ID and
descriptor hash for `testament.submit`; neither the schema hash nor a raw payload
digest is that descriptor hash.

The object requires nonblank `code` and `message` strings, at most 128 and 4096
decoded UTF-8 bytes. Optional `details` is a string up to 32768 decoded UTF-8 bytes
or `null`. The descriptor fixes which whitespace code points count as blank. The
complete encoded JSON is bounded at 64 KiB; unknown/duplicate fields and positional
arrays are invalid. Inline artifact payloads retain their separate 16-KiB bound;
larger valid reports use the transfer branch. Artifact admission proves shape and
custody, not the diagnosis's truth or the outcome of a claim.

If an older server refuses this schema, preserve the real evidence and exact
operation ID and report the unsupported capability. Use the exact saved request
for transport recovery. Never substitute fabricated test counts or change an
already-bound request's payload to make it pass admission.

## Before a mutation

Call `request.reserve` with no arguments and retain its returned `operation_id` before calling a mutation tool. Registration is automatic. Reservation creates no business work and is **not idempotent**: if its output is lost, use `request.pending` to discover outstanding IDs. An unprepared reservation can be inspected or explicitly sealed. Each separate mutation—submit, post, acquire, begin, attach, close—needs its own reserved ID. The JSON-RPC request ID only correlates a transport call.

The adapter durably binds the managed ID to the cluster, principal, ledger, operation name/version, normalized authored input and optional optimistic revision. It saves every generated object ID and the exact business envelope before transmission. Repeating an ID with changed intent returns an operation conflict. Retry uses that saved identity after a process restart. Existing unqualified 32-character lowercase hexadecimal IDs continue through their legacy epoch-one journals with permanent bindings; retain those IDs for their original operations.

## Interpret the result

Use MCP `structuredContent`, whose application envelope contains `schema_version`, `operation_id`, `condition` and `result`. A verified committed mutation receipt proves that particular ledger command. Read the actual claim lifecycle and requirements to establish satisfaction. Progress, artifact admission, testament generation, a receipt-only validation and the author's `confidence` are separate facts.

On an unknown outcome, cancellation or lost reply, preserve the operation ID. `request.inspect` reads the saved state; `request.retry` transmits only the saved pending request(s). Both take `operation_id`, and neither creates an unknown ID. Domain refusal remains a reported outcome with saved state. An incomplete/corrupt store fails closed: report the specific error and retained ID rather than automatically replacing it or deleting recovery files.

`request.inspect` also accepts optional `remote: true`. It queries the owner's retained receipt at a fresh quorum barrier, verifies the saved intent and any existing receipt, and leaves local recovery unchanged. Managed IDs query their exact stream generation and ordinal. A retained receipt proves the original outcome; `Retired` or `StreamClosed` fences execution without promising the historical result. `Unknown` leaves an earlier proposal able to commit. Preserve the original ID and use `request.retry` or an explicit seal to resolve uncertainty. Quorum loss produces an operational error, not a successful absent-receipt observation.

For a **legacy epoch request**, remote inspection queries its business key even if epoch admission is the locally pending step. If only that wire key is known, `request.status` accepts `epoch` and `request_id`; `request.epoch` accepts `epoch`. These legacy queries do not address managed IDs. The principal comes from authentication. A retained receipt takes precedence below the epoch floor. `BelowFloor` fences new admission while leaving historical commitment unknown; `Unknown` leaves an earlier proposal able to commit. Preserve saved receipts and exact retries under either result.

MCP cancellation ends client interest and can suppress the reply. It cannot retract an admitted write or cancel a claim. `claim.cancel` is the explicit, separately journaled business operation. This release has no durable parked-agent continuation or autonomous wake-up loop.

## Consume or resolve a managed result

After reading and retaining the original result and its object IDs, call `request.acknowledge` with its managed `operation_id`. This explicitly permits result retirement. Acknowledgments advance only over a contiguous prefix of consumed outcomes; an earlier unconsumed or unresolved operation remains protected. Inspection, cancellation and process exit do not acknowledge results. A retired ID returns `Retired` and can never create another mutation; its full historical result is no longer promised. A delayed cleanup error preserves the exact saved control for retry.

To end uncertainty deliberately, call `request.seal` with the original managed ID. It returns the earlier committed outcome if one exists; otherwise it commits a fence preventing that exact request from executing. Consume the returned outcome before acknowledging it. A seal resolves request admission; business claim cancellation remains `claim.cancel`. A normal refusal, Inform, timeout or absent receipt cannot substitute for this fence.

The human CLI allocates managed IDs internally and marks delivery only after successful output and explicit flush. Its automatic retirement uses a separate stream, so an unconsumed MCP window does not consume the CLI's capacity. For interrupted work, follow the copyable recovery command printed by the CLI or inspect its bounded pending list.

## Preserve read scope

List filters are optional and conjunctive. Preserve the same filters across continuations. List results contain `page.next.bytes`; encode those exact bytes as hexadecimal for the next tool's `cursor`. An empty page may still have a continuation. Treat list cursors as opaque scope/prefix proofs; an expired view requires a new read, with the prefix change made explicit.

`validation.get` returns its requirement plus bounded run summaries and verdict records. For another page, use the returned exact read token's sequence/route epoch as `prefix`. In the `ValidationResults` object's `next` position, map `run.target_hash`, `run.phase`, `run.epoch` and `attempt` to the flat `after` fields in the discovered schema; retain the original validation `id`. Nested results use the frozen wire representation: IDs/hashes are byte arrays and vocabulary values are numeric. Convert bytes to hexadecimal exactly. Version-1 validation phases are `1` → `admission`, `2` → `increment`, `3` → `whole_work`. A requirement with no runs is known but unexecuted.

`validation.context` is a read and takes no `operation_id`. It returns `result.context` containing `token`, `validation_id`, the pinned `validation`, its `claim`, an optional current `testament` (`id` and `value`), a bounded `records` page, and `next`. Use the same prefix/position conversion as `validation.get` for continuation. Its component reads share exactly one snapshot. An expired snapshot fails the complete call; explicitly start a new query if a fresh observation is needed. No lifecycle transition or managed request reservation occurs. Artifact bytes remain available through the artifact operations. Neither the current testament pointer nor an equal evidence schema identifies the target of a historical run or grants an execution lease.

## Transfer actual payloads

For a payload beyond the inline bound, retain a caller-chosen nonzero lowercase 32-hex `upload_id`, its exact byte length and BLAKE3 digest of the raw stream. This is a transfer identity, separate from a managed business `operation_id`. Call `upload.begin` with that metadata and the intended content class. Append contiguous chunks of at most 64 KiB using `upload.append` with byte `offset` and hexadecimal `bytes_hex`. Exact old bytes may be retried after response loss; altered metadata or bytes conflict. The returned `staged` and `received` counts distinguish owned local bytes from server progress.

Call `upload.seal` after all bytes are staged. Only the actual custody-gated server reply supplies a `reference`; reserve a separate operation ID and pass that exact reference to `artifact.submit` or `artifact.register`. Sealing content does not commit either domain operation. The current transfer bound is 64 MiB; structural attestation is bounded at 1 MiB for test-report and 64 KiB for error-report, so larger content storage does not imply artifact admission. Obtain the actual installed schema hash and capability before uploading evidence for it.

Retain the upload ID across process restarts. Resume through the same tools and exact bytes; `upload.begin` with identical metadata also reports saved progress. MCP cancellation stops the wait and preserves durable recovery. `upload.cancel` stops this journal’s transfer work and requests removal of current server staging without deleting immutable content, retracting an attachment or canceling a claim. On the current supporting server, cancellation persists terminal metadata before staging removal, so delayed Begin/Append/Seal cannot revive the scoped ID after restart. `cancel_acknowledged` remains a legacy-compatible RPC acknowledgment, not cross-version capability proof. Preserve terminal metadata in backups: the server bounds live plus terminal IDs at 65,536 and has no time-based reclamation. Future reclamation requires a generation fence; managed `request.seal` has a separate business-request admission contract. Saved local bytes and IDs remain quota-accounted after completion/cancel; capacity errors require preserving that history, not deleting initialized files.

Use `artifact.download` to read actual bytes. Start at offset zero, then retain its returned token and descriptor `content_hash`; supply the same token and next offset for every later page. Verify returned offset, advance by the byte count, and stop only at EOF. The service verifies the content manifest and stored chunks. An expired token fails explicitly; beginning at a fresh prefix is a separate decision. The artifact descriptor hash, content-manifest root and raw-stream digest are distinct values.

## Consultations and challenges

Use the original claim, disputed evidence and pinned requirements as the work contract. A **challenge** obliges the responding agent to deliver artifacts proving the challenger's stated claims. Identify each assertion and the artifact/check that establishes it. A response label, receipt, unsupported defense or empty proof cannot resolve the obligation.

When required challenge proof fails, the issuer or authorized corrective author evaluates the evidence and authors the appropriate corrective claims. Preserve the evidence and identify the concrete correction, responsible subject and acceptance checks. Where the current authenticated actor and available ordinary claim operations can author that work, create an explicit correction claim and retain its IDs. Otherwise report the outstanding corrective obligation to that participant. Participant-owned helpers for bounded retries/follow-ups and owner-checked parented claims remain incomplete; report a correction as created only when its actual committed receipt exists. The ledger does not invent a fix or create an observer job to author one.

A **consultation** requests satisfactory work under its stated quality bar. Complete that work with artifacts and a testament. The requesting participant normally authors a narrower follow-up consultation for remaining clarification or work; a distinct discovered defect can warrant an authorized correction. Keep references to the prior work and preserve its immutable history. The current tools expose the `consultation`, `challenge` and `correction` action vocabulary through ordinary claim submission; they do not supply a peer resolver or the owner-checked child-claim helper.

Quality checks require the actual pinned definition and an authorized evaluator invoking the corresponding capability in its own environment. Missing evidence, a negative verdict and failed tool execution remain distinct. Use the focal-evidence peer-evaluation branch for the implemented issuer acknowledgment, run initiation, fenced verdict and completion operations. Their actual authorization remains in the owner. Operator-only cluster tools, when advertised, are a separate administrative surface; contact, membership and credential changes do not prove data placement or achieved durability.


## Consume watch pages

For ongoing observation, discover `watch.open`, `watch.next`, `watch.acknowledge` and `watch.inspect`. Retain a caller-chosen name and its immutable filters before opening. Completion: `watch.open` returns one durably retained seed or tail page, with its delivery ID; retries use the same name/options.

Consume the entire page in the destination before calling `watch.acknowledge` with the name and hexadecimal delivery ID. Completion: `Consumed` records the durable local frontier; the next `watch.next` commits the source acknowledgment before advancing. Repeat the exact acknowledgment after response loss. An unconsumed `watch.next` returns the same delivery; `watch.inspect` never consumes. Keep seed objects, original delta facts and resolved/resync markers distinct from invented lifecycle events. Filtered empty seed pages still require consumption because their continuation advances.

A partial seed must retain its exact token and finish before its source lease expires. Report expiry explicitly; deliberately open a new named seed only after deciding how the destination handles overlap. Preserve the watch catalogue, initialized markers and adjacent managed allocator together. There are 16 names, one retained page per watch, 64-KiB pages and four outstanding managed cursor slots per watch. Neither implicit epoch changes nor filesystem deletion is recovery. CLI fallback: `focal watch claims`, `focal watch inspect --format json`, and the printed `focal ... watch resume NAME` command.
