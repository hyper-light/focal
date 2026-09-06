---
name: focal-claims
description: >-
  Manage Focal claims: author and post obligations, accept assigned work, record
  progress, inspect requirements and outcomes, or recover an interrupted mutation.
  Use focal-evidence when delivering artifacts and closing a testament.
---

Use the connected Focal MCP server. Read [the shared workflow contract](../references/workflow-contract.md) before the first call; it defines exact retries, authoritative outcomes, and bounded reads. Discover `tools/list`, including every `nextCursor` page, and use the returned schemas. The required operation versions are pinned in [the skill manifest](../manifest.json).

## Find the obligation

For a ledger-wide count, use `ledger.summary`. Its counters describe the selected
ledger at the returned committed prefix; avoid fetching every object to count it
or extrapolating those counts into global deployment health.

1. Read an exact claim with `claim.get`, or use its claim/source/target/status/action filters when exactly one result is required. A filtered get must prove uniqueness at one prefix; absence, ambiguity, expiry and exhausted visits are distinct outcomes. Use `claim.list` to survey multiple matches. Continue every needed page at the same query/prefix. Completion: identify the claim, issuer, subject, current lifecycle, scope and immutable requirements; an empty intermediate page is not absence.
2. Read its requirements with `validation.list`. Use `validation.context` to inspect one requirement, its claim, the current closing testament and a page of recorded runs at one consistent prefix; use `validation.get` when only the requirement and results are needed. Use `ledger.traverse` for bounded dependency or evidence exploration, preserving its opaque cursor and explicit stop reason. When responding to work, inspect the referenced evidence too, using `focal-evidence`. Completion: distinguish required acceptance conditions from advisory observations and know which evidence remains missing. Context is an observation, not permission to execute a check.

## Author and post

1. For a new obligation, supply `claim.submit` with the intended subject, complete description, action and acceptance requirements. A claim requires a required whole-work receipt validation. Substantive quality needs the actual pinned non-receipt handler IDs/versions and applicable evidence schemas; obtain these from the recorded contract and evaluator's available implementation. `self` resolves only the authenticated participant. Self-targeted claims must use the explicitly legal `handoff` action.
2. Reserve and retain one new `operation_id` using the shared workflow contract before this submission. Let the adapter allocate omitted object IDs once. Completion: the durable mutation receipt identifies the generated claim; retain its object IDs before acknowledging the consumed result.
For an atomic group of 1–64 authored claims, use `claim.submit_batch` with one reserved operation ID. Completion: one committed receipt identifies every generated claim; posting remains separate.

3. Post the generated claim with `claim.post`, using a separate operation ID. Completion: its committed result and a subsequent authoritative claim read establish the actual posted state. Generation alone does not dispatch work. For an authorized replacement obligation, use `claim.supersede` with its predecessor and complete successor; retain both identities and the committed lineage.

For a **consultation or challenge**, first read the [peer-work branch](../references/workflow-contract.md#consultations-and-challenges). The action vocabulary is available through ordinary `claim.submit`; automated peer routing, policy templates and corrective/follow-up issuance are not provided by this skill.

## Accept and perform assigned work

1. For a posted claim assigned to the authenticated participant, use `receipt.acquire` with the intended nonzero receipt epoch from current claim state. Use epoch one for a first acquisition. Completion: preserve the returned receipt ID and epoch as a pair; a stale or denied acquisition gives no standing to perform fenced mutations.
2. Record material progress with `claim.progress` under that exact receipt. Completion: the ledger records progress; the claim remains open until its lifecycle says otherwise.
3. After work completes or fails, use `focal-evidence` to deliver the actual outputs or diagnostics and author the response testament. Receipt acquisition records responsibility only. Report success only to the degree proved by the returned lifecycle and validation records. A committed submission, a complete-looking summary and a `confidence` label do not establish satisfaction.

## Cancellation and recovery

Use `claim.cancel` with a reason when business cancellation is intended and permitted by the current claim. Use MCP cancellation when ending only the client wait. Recover uncertain mutations with `request.inspect` and `request.retry` using the original operation ID, as specified in the shared contract. Completion: report the exact durable result or the remaining unknown state and recovery ID; never silently turn uncertainty into a second operation.

For the owner's current observation, use `request.inspect` with `remote: true` and the original operation ID. For a legacy epoch request whose wire key alone is known, use `request.status` with its `epoch` and `request_id`; `request.epoch` observes legacy admission and the epoch floor. Those two legacy queries do not address managed IDs. All observations are scoped to your authenticated participant. Preserve saved outcomes and follow the shared recovery contract for unknown or retired results.


## Follow ongoing changes

For a short observation, use `claim.wait` with the exact claim, the intended
`satisfied`, `terminal` or `released` predicate, and a timeout no longer than
30 seconds. Completion: `Met` means that predicate held at the returned prefix.
`Pending` means the observation deadline elapsed; retain the last observed fact
without calling it business failure. `Unmet` means a terminal claim cannot
satisfy the requested satisfaction predicate. This read does not reserve a
mutation ID, create a monitor or acknowledge any evidence. Prefer a durable
watch for longer observation rather than repeatedly restarting short waits.

When the task needs ongoing observation, follow the [watch consumption branch](../references/workflow-contract.md#consume-watch-pages). Use `watch.open`, `watch.next`, `watch.acknowledge` and `watch.inspect` only when discovered. Completion: the destination consumes each exact page before acknowledgment; unknown output remains retained and recoverable by name.

For a durable dependency wait owned by an active claim you issued, use
`monitor.register` with the exact owner, required `satisfied`, `terminal` or
`released` roots and an explicit timer/generation/deadline. Reserve its mutation
ID first. Completion: retain the committed monitor ID, then inspect it with
`monitor.get` after reconnect or when its dependencies change. A pending monitor
is a recorded wait, not running work. Its released sequence records release; it
does not by itself prove every root succeeded. Read the roots to establish the
actual outcome. A local timeout cannot fire its ledger deadline, expire the
monitor or issue corrective work. If the task lacks a trustworthy deadline,
use a bounded ordinary read or watch instead of fabricating timer authority.
