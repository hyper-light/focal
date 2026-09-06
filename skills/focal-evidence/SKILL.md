---
name: focal-evidence
description: >-
  Deliver or inspect Focal evidence: open receipt-fenced evidence sets, attach
  artifacts, transfer payloads, report completed or unsuccessful work in immutable
  testaments, submit peer verdicts, or prepare proof for a consultation or challenge.
---

Use the connected Focal MCP server. Read [the shared workflow contract](../references/workflow-contract.md) before the first call. Discover the actual tool schemas and all `tools/list` pages; the required operation versions are pinned in [the skill manifest](../manifest.json). Use `focal-claims` when authoring, posting or accepting the underlying claim.

## Establish the evidence contract

1. Read the claim with `claim.get` and its requirements with `validation.list`. For existing work, inspect `testament.list` and `artifact.list`, retaining their fixed-prefix continuations. Completion: know what the claim demands, which evidence already exists, and which required validations still need proof.
2. Confirm the current receipt ID and epoch from the committed acquisition and claim state. Acquire it through `receipt.acquire` only when this participant is the designated holder. Completion: the claim, receipt and intended evidence set refer to the same authorized work; stale fences require reconciling current state.
3. For consultation or challenge evidence, read the [peer-work branch](../references/workflow-contract.md#consultations-and-challenges). A challenge's response artifacts must prove the challenger's stated claims; evidence delivery alone cannot meet that quality obligation.

## Attach actual artifacts

1. Use `evidence.begin` under the current receipt, with its own reserved operation ID from the shared workflow contract. Completion: retain the committed evidence-set ID before acknowledging the consumed result or attaching anything.
2. Attempt the requested work and retain its actual outputs and diagnostics. When it completes or fails, follow the [reporting contract](../references/workflow-contract.md#report-completed-or-unsuccessful-work), including its installed diagnostic schema. Call `artifact.submit` with the claim, receipt, evidence set, artifact kind and exact schema hash. Small text or byte payloads are supported. For larger content, follow the [transfer branch](../references/workflow-contract.md#transfer-actual-payloads): `upload.begin`, `upload.append` and `upload.seal` retain exact transfer identity and bytes across retries. `upload.cancel` ends staging; `artifact.download` retrieves actual payload pages. Completion: the real server returns the immutable content reference before it is supplied to attachment. Schema discovery describes supported payloads; it does not provision a validator.
3. Wait for the artifact's committed receipt. Use `artifact.get` to retain its exact ID and returned descriptor `content_hash`; use `artifact.list` when surveying more than one artifact. Completion: every intended attachment exists under the evidence set and has a confirmed immutable descriptor hash. A raw payload digest or content-manifest root is a different value. For externally produced verdict proof outside the responder’s evidence set, `artifact.register` records an artifact under your authenticated producer identity. Registration gives it an ID/hash; it does not attach it to that response or supply missing receipt standing.

## Close one exact testament

1. Build the ordered `manifest` from those artifact ID/hash pairs. Include exactly the set's committed attachments in their required order, including the durable `kind: "error"` diagnostic required for every non-Complete account. Confirm all attachment outcomes before closing; recover any uncertain attachment using its original operation ID.
2. As the current respondent, author `testament.submit` after work completes or fails, with the same claim, receipt, evidence set, exact manifest, factual summary, confidence and explicit outcome. Give this close operation its own durable operation ID. Completion: a committed close identifies your immutable account; acquiring a receipt never creates it. Reuse the original `operation_id` for uncertain retries; preserve prior testaments and artifacts when later work changes.
3. Read `testament.get` and, when needed, `artifact.list` filtered by testament to inspect the frozen manifest. Use `validation.context` for a coherent requirement/claim/current-testament/result view, or `validation.get` for only recorded runs and verdict evidence. A context's historical runs can refer to earlier evidence; do not treat its current testament as their inferred target. Completion: distinguish testament generation, receipt acknowledgment, validation progress and claim satisfaction in the report. Read the peer-evaluation branch below when an acknowledgment or recorded evaluation is needed.

## Receive and evaluate as the authorized peer

For selecting an available external implementation, recovering its execution,
or inspecting historical targets, use [the validation skill](../focal-validation/SKILL.md).

1. As the immutable claim issuer, use `testament.receive` for the exact current closing testament, including an unsuccessful account. Completion: the committed acknowledgment is visible; receiving a response does not assert its substantive quality or author the respondent's testimony.
2. For whole-work checks use `validation.begin`; for an existing increment requirement use `validation.begin_increment` with its actual attached target and exact manifest. Inspect `validation.context` or `validation.get` afterward. Completion: obtain the recorded run’s target hash, phase, epoch, handler/version, attempt, manifest and applicable receipt fence. Preserve historical run targets; never derive a target from the current testament pointer alone.
3. The designated evaluator reads the exact work and diagnostic artifacts bound to that run, then invokes the pinned tool, skill or code in its own environment. Retain its actual proof as registered artifacts, then use `validation.submit` with that run’s complete fences and exact evidence ID/hash pairs. Completion: the committed verdict records the actual pass, fail, error or incomplete finding. A respondent's reported outcome is evidence to evaluate, not a supplied verdict. Tool output alone is not a committed verdict, and stale or differently assigned runs confer no standing.
4. As the issuer, use `validation.complete` when the recorded whole-work requirements permit it. Completion: read the actual resulting claim lifecycle. Core derives the permitted outcome from stored runs and graph constraints; the caller cannot declare satisfaction by assertion.

Keep missing proof, negative proof and failed execution distinct. Focal does not launch an evaluator or generate corrective work. These operations expose the current single-response reducer; independent per-artifact receipt/evaluation lifecycle records and symmetric result-testament history remain future storage work. Report only the exact durable facts available.


## Follow ongoing changes

When the task needs ongoing observation, follow the [watch consumption branch](../references/workflow-contract.md#consume-watch-pages). Use `watch.open`, `watch.next`, `watch.acknowledge` and `watch.inspect` only when discovered. Completion: the destination consumes each exact page before acknowledgment; unknown output remains retained and recoverable by name.

For contract discovery, use `validator.list` and `validator.get` to inspect actual pinned handlers and requirements recorded by claims. Completion: retain the exact handler/version and requirement context before external evaluation; discovery does not install a validator, launch code or attest execution.
