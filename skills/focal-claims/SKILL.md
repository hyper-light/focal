---
name: focal-claims
description: >-
  Manage Focal claims: author and post obligations, accept assigned work, record
  progress, inspect requirements and outcomes, or recover an interrupted mutation.
  Use focal-evidence when delivering artifacts and closing a testament.
---

Use the connected Focal MCP server. Read [the shared workflow contract](../references/workflow-contract.md) before the first call; it defines exact retries, authoritative outcomes, and bounded reads. Discover `tools/list`, including every `nextCursor` page, and use the returned schemas. The required operation versions are pinned in [the skill manifest](../manifest.json).

## Find the obligation

1. Read an exact claim with `claim.get`, or narrow `claim.list` using its discovered optional filters. Continue every needed page at the same query/prefix. Completion: identify the claim, issuer, subject, current lifecycle, scope and immutable requirements; an empty intermediate page is not absence.
2. Read its requirements with `validation.list` and actual runs/verdicts with `validation.get`. When responding to work, inspect the referenced evidence too, using `focal-evidence`. Completion: distinguish required acceptance conditions from advisory observations and know which evidence remains missing.

## Author and post

1. For a new obligation, supply `claim.submit` with the intended subject, complete description, action and acceptance requirements. A claim requires a required whole-work receipt validation. Substantive quality needs the actual pinned non-receipt handler IDs/versions and applicable evidence schemas; obtain these from the installed contract. `self` resolves only the authenticated participant. Self-targeted claims must use the explicitly legal `handoff` action.
2. Choose and retain one new `operation_id` for this submission before calling it. Let the adapter allocate omitted object IDs once. Completion: the durable mutation receipt identifies the generated claim; retain both that claim ID and operation ID.
3. Post the generated claim with `claim.post`, using a separate operation ID. Completion: its committed result and a subsequent authoritative claim read establish the actual posted state. Generation alone does not dispatch work.

For a **consultation or challenge**, first read the [peer-work branch](../references/workflow-contract.md#consultations-and-challenges). The action vocabulary is available through ordinary `claim.submit`; automated peer routing, policy templates and corrective/follow-up issuance are not provided by this skill.

## Accept and perform assigned work

1. For a posted claim assigned to the authenticated participant, use `receipt.acquire` with the intended nonzero receipt epoch from current claim state. Use epoch one for a first acquisition. Completion: preserve the returned receipt ID and epoch as a pair; a stale or denied acquisition gives no standing to perform fenced mutations.
2. Record material progress with `claim.progress` under that exact receipt. Completion: the ledger records progress; the claim remains open until its lifecycle says otherwise.
3. Deliver actual work through `focal-evidence`. Report success only to the degree proved by the returned lifecycle and validation records. A committed submission, a complete-looking summary and a `confidence` label do not establish satisfaction.

## Cancellation and recovery

Use `claim.cancel` with a reason when business cancellation is intended and permitted by the current claim. Use MCP cancellation when ending only the client wait. Recover uncertain mutations with `request.inspect` and `request.retry` using the original operation ID, as specified in the shared contract. Completion: report the exact durable result or the remaining unknown state and recovery ID; never silently turn uncertainty into a second operation.

For the owner's current observation, use `request.inspect` with `remote: true` when a saved operation ID is available. If only the wire request key is known, use `request.status` with its `epoch` and `request_id`; `request.epoch` observes admission and the floor for your requested epoch. These queries are read-only and scoped to your authenticated participant. An unknown result cannot authorize replacement work. A below-floor result fences new admission but leaves the historical outcome unknown; a retained committed receipt takes precedence even below the floor. Preserve existing journal receipts and follow the shared recovery contract.
