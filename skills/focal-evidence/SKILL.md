---
name: focal-evidence
description: >-
  Deliver or inspect Focal evidence: open receipt-fenced evidence sets, attach
  artifacts, close immutable testaments, inspect validation results, or prepare
  proof for a consultation or challenge.
---

Use the connected Focal MCP server. Read [the shared workflow contract](../references/workflow-contract.md) before the first call. Discover the actual tool schemas and all `tools/list` pages; the required operation versions are pinned in [the skill manifest](../manifest.json). Use `focal-claims` when authoring, posting or accepting the underlying claim.

## Establish the evidence contract

1. Read the claim with `claim.get` and its requirements with `validation.list`. For existing work, inspect `testament.list` and `artifact.list`, retaining their fixed-prefix continuations. Completion: know what the claim demands, which evidence already exists, and which required validations still need proof.
2. Confirm the current receipt ID and epoch from the committed acquisition and claim state. Acquire it through `receipt.acquire` only when this participant is the designated holder. Completion: the claim, receipt and intended evidence set refer to the same authorized work; stale fences require reconciling current state.
3. For consultation or challenge evidence, read the [peer-work branch](../references/workflow-contract.md#consultations-and-challenges). A challenge's response artifacts must prove the challenger's stated claims; evidence delivery alone cannot meet that quality obligation.

## Attach actual artifacts

1. Use `evidence.begin` under the current receipt, with its own durable operation ID. Completion: retain the committed evidence-set ID before attaching anything.
2. Produce the requested material, then call `artifact.submit` with that claim, receipt, evidence set, artifact kind and exact installed schema hash. Use the discovered payload shape. Small text or byte payloads are supported; a content reference must identify already durably stored content. This adapter has no upload, download or schema-registration tool. Obtain required larger-content storage or schema provisioning through the actual available host path; report an unmet capability when it is unavailable.
3. Wait for the artifact's committed receipt. Use `artifact.get` to retain its exact ID and returned descriptor `content_hash`; use `artifact.list` when surveying more than one artifact. Completion: every intended attachment exists under the evidence set and has a confirmed immutable descriptor hash. A raw payload digest or content-manifest root is a different value.

## Close one exact testament

1. Build the ordered `manifest` from those artifact ID/hash pairs. Include exactly the set's committed attachments in their required order. Confirm all attachment outcomes before closing; recover any uncertain attachment using its original operation ID.
2. Use `testament.submit` with the same claim, receipt, evidence set, exact manifest, factual summary, confidence and outcome. Give this close operation its own durable operation ID. Completion: a committed close identifies an immutable testament. Reuse the original `operation_id` for uncertain retries; preserve prior testaments and artifacts when later work changes.
3. Read `testament.get` and, when needed, `artifact.list` filtered by testament to inspect the frozen manifest. Read `validation.get` for actual execution and verdict evidence. Completion: distinguish testament generation, receipt acknowledgment, validation progress and claim satisfaction in the report. These tools neither schedule validators nor author runtime completion.

Keep failed checks and residual work visible in the evidence. An evaluator error, missing proof and negative proof are different findings. If the service has no running validator/worker for the required next transition, leave that transition pending and name the missing integration; a tool return cannot replace it.
