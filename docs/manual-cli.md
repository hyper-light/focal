# Manual CLI

See [cluster administration](cluster-admin.md) for authenticated local operator inspection, root and application membership, invitation management, and exact admin-request recovery.

Install the [prebuilt `focal` binary](../README.md#install) for your platform. The
same executable supplies the server, human CLI and MCP server; no source checkout
or compiler is required to run it. The [release procedure](../scripts/release/README.md)
describes downloadable CI artifacts while the first tagged release is pending.
[Building from source](building.md) is an optional contributor workflow.

The `focal` binary connects to the running local service through its authenticated Unix socket. Start it in one terminal:

```sh
focal --data-dir /tmp/focal-manual start
```

Use the same `--data-dir` in another terminal. The examples below abbreviate that common option. `--config FILE` and `--data-dir DIR` work before or after subcommands. `--help` lists the flags at each level.

## Offline schemas, examples and completion

These commands need no running service, node identity or saved configuration. They
do not create client journals or reserve request identities:

```sh
focal schema list
focal schema list --format json
focal schema get claim.submit
focal schema get claim.submit --direction input
focal schema get claim.submit --direction output
focal schema example claim.submit > claim.json
focal schema get test-report
focal schema get error-report
focal schema get domain-registry
```

Operation names and input/output schemas come from the shared authored operation
registry used by the clients and MCP adapter. Input is the authored document, with
human enum names and IDs; it does not contain authentication, request-envelope
fields or trusted authority. The output schema describes the shared
`ApplicationResult` envelope, not the human CLI table, CLI journal output or JSON-RPC
transport wrapper. Nested frozen wire results still require their typed decoder's
semantic checks, as the schema descriptions state. Built-in test-report and
domain-registry output remain compatible; error-report supplies a bounded diagnostic
contract. `--direction` applies only to operations.

Examples pass through the real typed input decoder and serializer, including
defaults. The claim example is a self-targeted handoff with a pure receipt
requirement, suitable for trying the local protocol; it does not claim substantive
quality checking. You can submit it using `focal submit claim --file claim.json`.
Other examples contain illustrative existing-object IDs; replace them with the
actual claim, receipt, evidence-set and artifact references before sending. An
empty example testament manifest is valid only when its actual evidence set is
empty and its acceptance contract permits it. The artifact example contains the
real built-in test-report schema hash. A validation-result example's run, handler,
version, manifest, receipt and proof references are illustrative: copy the real
committed context and evidence instead. Required optimistic revisions belong to
the mutation envelope (`--expected-revision`), not these authored documents.
Discovery does not promise server admission.
`schema list --format json` marks `example_available` for each released operation;
an unavailable example is an error, never a fabricated request.

Generate completion scripts from this binary's actual Clap command tree:

```sh
focal completion bash > focal.bash
source ./focal.bash

focal completion zsh > _focal
focal completion fish > focal.fish
focal completion powershell > focal.ps1
focal completion elvish > focal.elv
```

Load the generated file using your shell's normal completion setup. Scripts include
implemented subcommands and flags. Help and completion show only the filters
supported by the selected object family; explicitly supplied unsupported filters
still receive the shared typed input error. Bash and Zsh also include positional argument
values such as schema names. Positional value support varies in the other shell
generators. They do
not query the ledger or suggest secret values. Regenerate after upgrading the
binary. All discovery output uses fallible writes with a one-MiB limit; a closed
output pipe reports an error without opening node state.

## Submit a claim

Each claim needs an explicit acceptance contract, including a required whole-work receipt validation. `--target` names the subject participant. IDs are 32 hexadecimal characters; hashes are 64. `self` resolves only the authenticated local participant and is accepted in participant fields. The domain prohibits self-targeted `work`; the following local handoff uses the explicitly permitted `handoff` action.

```sh
focal submit claim \
  --target self --action handoff \
  --description 'Deliver the checked report' \
  --scope file:report.json \
  --validation-json '{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}'
```

The result includes the generated claim ID. Request registration, identity allocation and receipt cleanup happen automatically; no extra flags are required. This generates the claim; `focal claim post CLAIM_ID` makes it actionable. A receipt requirement checks receipt of the testament. Substantive work acceptance requires additional pinned validation definitions. Participant-authored challenge, consultation and corrective/follow-up helpers remain planned in [P20](archictecutre/13-cli-and-agent-implementation-plan.md), refined by the [peer validation contract](archictecutre/16-peer-validation-contract.md). The participant authors that work; Focal records and checks its authorized mutations.

JSON and YAML use the same authored document and the same Rust builder:

```sh
focal submit claim --json '{"target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}'

focal submit claim --yaml 'target: self
action: handoff
description: Deliver the checked report
validations:
  - kind: receipt
    phase: whole_work
    mode: required
    description: Receive the report testament
    evaluator: self'

focal submit claim --file claim.yaml
focal submit claim --file - --input-format json
```

Each fresh invocation generates a new occurrence and request identity. If a submission is interrupted, copy its printed `focal request retry --operation-id m1:…` command. Retrying the same managed ID preserves the original command; an acknowledged ID reports `Retired` and cannot execute again. Supplying the same explicit IDs and authored fields yields the same canonical command across flags, JSON and YAML; it does not replace the durable request identity.

Choose field flags or one document. Documents cannot override issuer, trusted cause, runtime authority, lifecycle or custody. Unknown fields, duplicate keys, YAML aliases/tags/multiple documents, numeric vocabulary codes, excessive nesting and over-budget input are rejected. A document is limited to 256 KiB. Validation definitions can also be supplied with repeated `--validation-file FILE`; aggregate definition bytes are bounded. Detailed authored fields are defined by [the shared DTOs](../crates/focal-client/src/input/documents.rs).

## Deliver artifacts and a testament

The current receipt holder is the respondent and authors the testament after its
work completes or fails. Acquiring a receipt records responsibility; it never
creates a testament. The respondent supplies the summary, outcome and exact
evidence references. Use the IDs printed by each preceding step:

```sh
focal claim post CLAIM_ID
focal receipt acquire CLAIM_ID
focal claim progress CLAIM_ID --receipt RECEIPT_ID --receipt-epoch 1 --message 'Report checked'
focal evidence begin --claim CLAIM_ID --receipt RECEIPT_ID --receipt-epoch 1
focal schema get test-report
```

The built-in schema command prints the exact test-report schema hash, descriptor and an example. Attach a report using that hash and the returned evidence-set ID:

```sh
focal submit artifact --claim CLAIM_ID \
  --receipt RECEIPT_ID --receipt-epoch 1 --evidence-set EVIDENCE_SET_ID \
  --kind test-report --schema-hash SCHEMA_HASH \
  --text '{"passed":1,"failed":0,"skipped":0}'

focal submit testament --claim CLAIM_ID \
  --receipt RECEIPT_ID --receipt-epoch 1 --evidence-set EVIDENCE_SET_ID \
  --artifact ARTIFACT_ID:DESCRIPTOR_HASH \
  --summary 'Checked report attached' --confidence committed --outcome complete
```

For unsuccessful work, use this alternative in place of the successful report and
testament above. The error-report schema describes tool failures, refusals and
interruptions without assuming any test ran. Obtain its exact hash with
`focal schema get error-report` and substitute it for `ERROR_SCHEMA_HASH`:

```sh
focal submit artifact --claim CLAIM_ID \
  --receipt RECEIPT_ID --receipt-epoch 1 --evidence-set EVIDENCE_SET_ID \
  --kind error --schema-hash ERROR_SCHEMA_HASH \
  --text '{"code":"tool_unavailable","message":"The required tool could not run","details":"No test result was produced"}'

focal submit testament --claim CLAIM_ID \
  --receipt RECEIPT_ID --receipt-epoch 1 --evidence-set EVIDENCE_SET_ID \
  --artifact ERROR_ARTIFACT_ID:ERROR_DESCRIPTOR_HASH \
  --summary 'The tool could not run; the failure diagnostic is attached' \
  --confidence committed --outcome failed
```

Use a truthful code/message and the returned artifact ID and descriptor hash.
`code` and `message` are required nonblank strings, limited to 128 and 4096 UTF-8
bytes. Optional `details` is a string up to 32768 UTF-8 bytes or `null`. The complete
JSON payload is limited to 64 KiB; unknown and duplicate fields are rejected. A
successful schema check verifies this shape, not whether the diagnosis is true.

For tests that actually ran and failed, the original test-report schema remains
usable: submit `--kind error --schema-hash SCHEMA_HASH` with real counts such as
`--text '{"passed":0,"failed":1,"skipped":0}'`. Use the corresponding returned
artifact reference in the testament. Do not invent test counts for a tool that
could not execute.

Every non-`complete` outcome—`partial`, `refused`, `impossible`, `interrupted` or
`failed`—requires at least one durable `kind=error` artifact produced by the current
holder under this claim's current receipt and evidence set. A summary alone is
insufficient. Partial outputs can accompany the diagnostic; include **every**
staged artifact in the exact original order when closing the set. `committed`
expresses confidence in the reported account, including a failure account; it
does not declare the work successful. The current storage model permits one
closing testament per claim, so these success and failure examples are alternatives.

The claimant receives the failure testament and its designated evaluator checks
the exact diagnostic evidence under the claim's declared requirements. Read its
manifest and retrieve the referenced bytes before evaluation:

```sh
focal get testament TESTAMENT_ID --format json
focal list artifacts --testament TESTAMENT_ID
focal get artifact ERROR_ARTIFACT_ID --output ./error-report.json
```

Continue with the receive and validation steps below. The reported `failed`
outcome does not itself submit a validation verdict or bypass those steps. Declare
substantive test or inspection requirements when authoring the claim: the pure
receipt requirement in the introductory example proves delivery only.

`--artifact` repeats in manifest order; `--manifest-file FILE` accepts a JSON/YAML array of `{id, hash}` references. A testament can instead be supplied with `--json`, `--yaml` or `--file`:

```yaml
claim: CLAIM_ID
receipt:
  id: RECEIPT_ID
  epoch: 1
evidence_set: EVIDENCE_SET_ID
manifest:
  - id: ARTIFACT_ID
    hash: DESCRIPTOR_HASH
summary: Checked report attached
confidence: committed
outcome: complete
```

Artifacts accept the same document modes. An inline payload document is `{type: text, text: ...}` or `{type: inline, bytes: [...]}`; a content payload contains an existing immutable content reference. Inline payloads and opaque metadata are each limited to 16 KiB. `submit artifact --payload-file FILE` stages the complete file in a private durable transfer journal, uploads it through custody, then attaches its immutable reference. Interrupted retries preserve the exact staged bytes even if the original file changes or disappears. The transfer limit is 64 MiB; this release's built-in test-report schema attestation accepts at most 1 MiB. Transfer capacity does not bypass schema or custody admission. `artifact register --payload-file FILE` uses the same durable 64-MiB staging/upload path before registering independent proof; its schema admission remains subject to the actual installed validator bound.

Schema meaning and the chosen validating tool or skill are a participant contract.
The schema hash pins the payload shape; `kind` names the artifact's role. Thus a
test-report payload with failing counts or a generic error-report can be a typed
`error` artifact. Current service ingress admits those two pinned schemas. Arbitrary schema
registration is not yet exposed: a participant's agreement or a supplied hash
cannot install a schema validator or assert trusted custody/schema validity.

Inspect or cancel a saved transfer using the `upload_id` reported by an interrupted upload:

```sh
focal artifact upload inspect UPLOAD_ID --format json
focal artifact upload cancel UPLOAD_ID --format yaml
focal artifact upload inspect UPLOAD_ID --origin mcp
```

`UPLOAD_ID` is the exact nonzero 32-character lowercase hexadecimal transfer ID, not the artifact ID or managed operation ID. The default `--origin cli` selects automatic file-transfer history; `--origin mcp` selects uploads initiated by that adapter in the same client context. Inspection is local and sends nothing. Neither command creates a missing upload or store. Preserve the selected context and its private history.

Cancellation persists its exact request before transmission. `CancelPending` means cancellation was saved but no acknowledgment is recorded; repeat the same cancel command after uncertainty. `CancelAcknowledged` includes `progress.cancel_acknowledged: true`; supporting servers retain a terminal ID fence across restart. This acknowledgment is not a cross-version capability proof. A sealed `progress.reference` stays present, and committed artifacts/content are retained. Transfer cancellation does not cancel a claim, erase a receipt, or free the reserved domain operation: use `request.seal` separately if abandoning an uncommitted managed mutation. The diagnostic prints a copyable recovery command with the original data directory and selected client context.

Closing the testament records `TestamentGenerated`. It does not by itself acknowledge
the testament, run validators or establish satisfaction. The issuer can explicitly
receive the closing response and begin its eligible evaluation:

```sh
focal testament receive TESTAMENT_ID --claim CLAIM_ID
focal validation begin --claim CLAIM_ID
focal get validation VALIDATION_ID --context --format json
```

The issuer/designated evaluator invokes its validating tool, skill or code in its
own environment. Beginning evaluation records the pinned run; Focal launches no
agent or worker. After producing actual result evidence, register that evidence
as yourself and submit your verdict against the exact observed run:

```sh
focal artifact register --kind test-report --schema-hash SCHEMA_HASH \
  --payload-file ./validator-result.json

focal submit validation --validation VALIDATION_ID \
  --target-hash TARGET_HASH --phase whole_work --epoch RUN_EPOCH \
  --handler HANDLER_ID --handler-version HANDLER_VERSION --attempt ATTEMPT \
  --manifest MANIFEST_HASH --receipt RECEIPT_ID --receipt-epoch RECEIPT_EPOCH \
  --value pass --evidence RESULT_ARTIFACT_ID:RESULT_DESCRIPTOR_HASH

focal validation complete --claim CLAIM_ID
```

Use the real outcome (`pass`, `fail`, `incomplete` or `error`) and the committed
run's exact handler, target, manifest, epoch and attempt. `--agentic` must match its
pinned handler and grants no authority. Omit receipt fields only for a run whose
context actually requires no receipt. Repeat `--evidence` for multiple committed
proof artifacts. Registering evidence does not insert it into the respondent's
already-closed manifest. `artifact register` supports inline `--text`, bounded
`--payload-file`, optional `--metadata-file`, repeated typed `--input-json`
references and `--visibility`; the service still verifies its supported schema and
custody. `submit validation` and `artifact register` also accept one strict
`--json`, `--yaml` or `--file` authored document instead of field flags.

For a failure testament, validate its exact error artifact just as you validate
successful work evidence. A conclusive failed test supports `fail`; `error`
describes an evaluator/tool failure to establish the result. Keep those separate
from the respondent's reported outcome and artifact kind. The designated evaluator
(which may be the claimant) records the actual authorized verdict and its own proof;
Focal does not manufacture either participant's account.

`testament receive`, `validation begin` and `validation complete` use the observed
claim revision when `--expected-revision` is omitted. That fence is persisted with
the prepared request; an exact retry does not silently substitute a newer revision.
Receipt does not prove quality, and complete derives the actual recorded Required
outcomes and graph predicates. A recorded failing verdict is a successful write of
that result, not an assertion that the work passed. These operations expose the
current lifecycle model; the independent artifact/testament state migration in
[17](archictecutre/17-lifecycle-state-and-authority.md) remains separate work.

To correct or replace work, create an explicit successor without rewriting the
predecessor's history:

```sh
focal claim supersede CLAIM_ID --file successor-claim.json
```

The successor uses the same claim field flags or claim JSON/YAML document as
`submit claim`; the predecessor is the positional ID. An optional `--id` names the
new successor. The registry's `claim.supersede` schema wraps these as `predecessor`
and `successor`, while this CLI spelling supplies the predecessor separately.
`focal claim cancel CLAIM_ID --reason TEXT` remains explicit business cancellation,
subject to standing and lifecycle rules. The optional `focal demo` uses the Rust
embedding helper under exclusive ownership; it is not required for peer evaluation.

## Get and list

```sh
focal get claim CLAIM_ID
focal get claim --source PARTICIPANT_ID --target PARTICIPANT_ID
focal get testament TESTAMENT_ID
focal get artifact ARTIFACT_ID
focal get artifact ARTIFACT_ID --output ./report.json
focal get validation VALIDATION_ID
focal get validation VALIDATION_ID --context

focal list claims
focal list testaments
focal list artifacts
focal list validations
focal list claims --source self --status generated
focal list testaments --claim CLAIM_ID
focal list artifacts --testament TESTAMENT_ID
focal list validations --claim CLAIM_ID
```

Every list filter is optional and applies within the selected authorized ledger. Supplied filters combine with AND. Unsupported combinations are errors. `--source` means claim issuer and never changes authentication. A singular filtered `get claim` proves that exactly one object matches at a fixed prefix; two matches are an ambiguity error. If its bounded query budget expires, narrow the filters or use a list.

| Family | Implemented filters |
| --- | --- |
| Claims | `--claim`, `--source`/`--issuer`, `--target`/`--subject`, `--status`, `--action`, `--scope`, `--relation`, `--caused-by` |
| Testaments | `--claim`, `--outcome`, `--confidence` |
| Artifacts | `--claim`, `--testament`, `--producer`, `--kind`, `--schema-hash`, `--input` |
| Validations | `--claim`, `--evaluator`, `--kind`, `--phase`, `--mode` |

All four also accept exclusive `--created-after` and inclusive `--created-through` creation `SessionSeq` bounds.

`list artifacts --testament` follows the immutable manifest, so a related artifact outside that manifest is excluded. `get artifact --output FILE` streams content in at most 64 KiB pages and atomically publishes a new private file after complete retrieval and fsync. Existing files, directories and symlinks are never overwritten. The service verifies the addressed manifest and each stored chunk; the CLI checks exact offsets, length and EOF. The manifest root is not a digest of concatenated raw bytes. Independent client-side manifest proof export remains open. A publication fsync failure may leave a complete output file while returning an error; it never reports success for partial content.

Lists return at most 100 matches by default; `--limit` accepts 1–256. Each service page also bounds records visited and encoded bytes. Copy the returned `CURSOR` into `--cursor`, retaining the same filters. A page with no matches may still have a cursor. Server list cursors bind authentication scope, query and exact prefix, and expire with snapshot retention. An expired or invalidated prefix requires starting a new read.

`get validation` returns the requirement plus bounded run summaries and committed verdict attempts, including evaluator, handler, manifest, target, epoch, attempt and evidence references in JSON. Its `--limit` and `--cursor` page through that one requirement at the same prefix. A known requirement with no execution has an empty records array; a missing requirement returns not found. These reads do not start validation work.

Add `--context` to include the owning claim and its current closing testament, with the testament's exact artifact manifest. The human output labels claim status, testament generation and testament acknowledgment separately. `--format json` returns `context` and an optional `cursor`; the context contains the token, validation ID, pinned requirement, claim, optional testament (`id` and `value`), records and next position. Continue with `--context --cursor CURSOR`. Each component comes from the same snapshot, even if writes occur between reads. Snapshot expiry fails the whole read; omit the cursor explicitly to begin again at a fresh snapshot. Context requires at most three read calls, bounded total response size and one overall deadline.

The current testament is not necessarily the target of an older recorded run. Context does not choose an artifact by schema, acquire a work receipt, grant an execution lease or advance any lifecycle. Use `get artifact` to inspect the manifest's evidence. The independent artifact/testament state model remains implementation work in the [peer contract](archictecutre/16-peer-validation-contract.md).

## Summarize the selected ledger

```sh
focal ledger summary
focal ledger summary --format json
focal ledger summary --format yaml
focal schema get ledger.summary --direction output
```

This returns committed counts for claims, testaments, artifacts, validations, evidence sets and validation runs in the selected ledger. The service copies scalar map lengths after a fresh quorum read; it does not download or scan the graph. JSON/YAML includes `summary.token` (ledger, sequence and route) and `summary.applied_index`. Repeated reads consume no mutation IDs or managed ordinals. Counts include all retained canonical records; they do not classify lifecycle outcomes or describe other ledgers. The token identifies this observation without retaining a historical snapshot lease. No filters, cursor or saved-prefix input are accepted.

## Traverse the graph

```sh
focal ledger traverse claim:CLAIM_ID --edge requirement --depth 1 --limit 32
focal ledger traverse claim:CLAIM_ID artifact:ARTIFACT_ID --direction reverse --limit 16 --format json
focal ledger traverse claim:CLAIM_ID --edge requirement --depth 1 --limit 32 --cursor CURSOR
focal schema get ledger.traverse --direction input
```

Traversal follows existing object-target edges in deterministic breadth-first
order. Roots are typed and limited to 32. `--edge` can repeat: use a canonical
relation such as `depends_on`, or `requirement`, `testament_of`, `evidence`,
`artifact_input`, and `validation_of`. Omitting it includes all indexed edge
kinds. Participant, action and root-command targets are not extra object families
and are not returned as graph objects.

Each page carries an immutable read token, visited-edge counts and an explicit
stop reason. `Complete` means the selected traversal completed. `PageLimit`
returns a cursor, including on pages with no objects when edge work consumed the
page budget. `DepthLimit`, `NodeLimit`, `EdgeLimit` and `StateLimit` report bounded
truncation. Preserve every query option when using `--cursor`; changing the query
is rejected. Repeating a cursor returns the same page even after later mutations.

Use `--max-visits` and `--max-bytes` for page work/output bounds, and `--max-nodes`
and `--max-edges` for cumulative traversal bounds. The server retains at most 32
continuation states under its memory budget. Cursors expire with the 30-second
snapshot lease and after the serving owner restarts; restart the query when its
snapshot expires. This is a bounded graph read, not a download of the whole ledger
or a lifecycle-history reconstruction. MCP `ledger.traverse` and the Rust SDK use
the same query and page contract.

## Native engine verbs

A ledger activated on the native engine (`focal cluster replicas activate-native`,
[cluster-admin.md](cluster-admin.md)) answers the same verbs through the native
wire profile. The CLI probes the engine once per invocation with a standing
read; on a native ledger it compiles every document into one exact `FCNINPUT`
frame, journals it under an `n1:` reference before sending, and resends the
identical bytes until the owner commits or refuses it. Native and V1 differ in
what a document may say, so the native descriptors are version 2 of the same
names (`focal schema get claim.submit --native`, `focal schema coverage`).

The two-party cycle on a native ledger:

```text
# issuer: one required receipt (delivery) check plus one programmatic check on slot 0
focal submit claim --description 'Run the suite.' --target <ALICE> \
  --validation-json '{"kind":"receipt","description":"Record delivery.","deadline":{"at":4102444800000}}' \
  --validation-json '{"kind":"test","description":"The suite passes.","target":{"type":"slot","index":0,"name":"report"},"evaluator":"self","handlers":[{"id":<HANDLER>,"version":<VERSION>}],"deadline":{"at":4102444800000}}' \
  --slot-json '{"slot":0,"checks":[{"declaration":1}]}' --format json
focal claim post <CLAIM>
# respondent (an enrolled client context)
focal --client-context alice receipt acquire <CLAIM>
focal --client-context alice artifact submit --claim <CLAIM> --slot 0 --text '{"passed":3,"failed":0,"skipped":0}'
focal --client-context alice testament submit --claim <CLAIM> --summary 'Suite passed.' \
  --confidence committed --outcome complete --slot 0=<ARTIFACT>:<HASH>
focal --client-context alice testament post <TESTAMENT> --claim <CLAIM>
# issuer receives, evaluates and reports; acceptance is derived by the owner
focal testament receive <TESTAMENT> --claim <CLAIM>
focal validation begin --claim <CLAIM> --validation <VALIDATION>
focal validation report --claim <CLAIM> --validation <VALIDATION> --verdict pass --text '{"passed":3,"failed":0,"skipped":0}'
focal get claim <CLAIM>
```

A native claim may cite exact evidence and carry a follow-up policy. A
`reviews` or `derived_from` relation may target `artifact:ID@HASH`, the
artifact at its committed descriptor hash (`--relation
reviews:artifact:<ARTIFACT>@<HASH>`, or `"relations":[{"kind":"reviews","target":"artifact:…@…"}]`
in a document); the owner refuses an unknown artifact, a different hash or
any other relation kind naming evidence, and a pending artifact cannot be
cited. A document's `policy` (`corrective_allowed`, `max_follow_ups` up to
1,024, `single_issuer`, `escalation` of `none`, `holder` or `evaluator`) is
authored immutably with the claim and read back by `get claim`. Either
selects descriptor schema 2; every other claim keeps schema 1, so existing
identities and hashes are unchanged.

The owner admits peer follow-ups under that policy. A correction is a claim
with `--action correction`, `--relation invalidates:claim:<CHALLENGE>` and
`--relation reviews:artifact:<REPORT>@<HASH>` naming the report of the
challenge's failed verdict; the challenge must allow corrections, the report
must be its terminal Fail, Incomplete or Error verdict at the current
registration generation, the author must be the challenge's issuer, its
holder (unless escalation is `none`) or, under `escalation: evaluator`, the
evaluator who reported that verdict, and `single_issuer` refuses a second
correction (`conflicting_cause`, exit 5). The other refusals are typed too:
`invalid_target` for a claim that is no challenge (exit 2), `invalid_policy`
when its policy forbids corrections (2), `missing_evidence` when the cited
artifact is not its verdict (2), `invalid_transition` when the verdict
passed or may still be retried (5), `stale_evaluation` when the challenge
was re-registered since (5) and `unauthorized` for anyone else (3). A
follow-up consultation is a claim with `--action consultation` and
`--relation refines:claim:<CONSULT>`; the refined consultation's
`escalation` names who may file it (`unauthorized`) and `max_follow_ups`
bounds how many (`invalid_policy`, exit 2). Neither reopens the claim it
follows.

The peer verbs package these shapes; each is an authored shape of `submit
claim` with the same frame, `n1:` identity and receipt:

```text
focal claim challenge --target <ALICE> --description 'Prove the report covers the edge cases.' \
  --artifact <ARTIFACT>[@<HASH>] --validation-json '...' --slot-json '...' \
  --policy-json '{"corrective_allowed":true,"max_follow_ups":1,"single_issuer":true,"escalation":"evaluator"}'
focal claim consult --target <ALICE> --description 'Which cases does the parser leave undefined?' \
  --validation-json '...' --policy-json '{"max_follow_ups":2,"escalation":"holder"}'
focal claim correct --challenge <CHALLENGE> --verdict <REPORT>[@<HASH>] \
  --description 'Redo the inspection with the missing cases.' --validation-json '...'
focal claim follow-up --refines <CONSULT> --description 'And the unicode cases?' --validation-json '...'
focal claim lineage <CLAIM> --format json
focal claim wait <CLAIM> --until testament --timeout-ms 10000
```

`claim challenge` needs `--policy-json`; `--artifact` names the disputed
artifact, whose hash is read from the ledger when omitted. `claim correct`
cites the report artifact of the challenge's failed verdict (read it from
`get claim`: the evaluation's `last_result.evidence`); `--target` defaults
to the challenge's subject, and the correction's occurrence identity derives
from the challenge, the verdict and you, so the same correction sent twice
is one claim. `claim follow-up` refines a committed consultation and
defaults its target to that consultation's subject; its identity derives
from the refined claim, the query and you. Every verb also takes the
document form (`--json`, `--yaml`, `--file`) with the same fields as the MCP
tools. `claim lineage` prints one page of committed claims: the claim with
its content, its `caused_by` ancestors nearest first, then the corrections
that invalidate it, the consultations that refine it and the children it
caused, each with its content and all read at or after the first read's
token. `claim wait` observes a native claim like the V1 observer (31 probes,
one second apart, at most 30 seconds) and adds `--until testament`, met once
the issuer has received a closing testament.

Every mutation prints `{"schema_version":2,"operation_id":"n1:…","condition":"Committed","result":{"kind":"native","receipt":…,"created":[…]}}`
(`--format json`); `created` lists the identities the frame minted (claim,
validation, receipt, artifact or testament). A closed refusal prints
`condition` from its category with the owner's detail and exits with the
matching class (invalid input 2, unauthorized 3, not found 4, stale or
conflicting 5, capacity 6); a pending ticket or a lost reply exits 7 with a
`Recovery:` line naming `focal request retry --operation-id n1:…`, which
resends the exact journaled frame and prints the receipt once it commits. A
committed receipt is durable in the journal before it is printed, so a reply
lost on a broken pipe is found with `focal request pending` and reprinted by
the same retry. `focal request inspect --operation-id n1:…` shows the
recorded receipt or refusal without sending anything; with `--remote` it
reads the owner's committed outcome for that request key instead, which
also observes an operation the MCP adapter journaled under the same context
([mcp.md](mcp.md#native-engine-tools)).

Reads on a native ledger return native documents: `focal get claim ID` (with
its content, scopes, responses and evaluations), `focal get testament ID`,
`focal get artifact ID` and `focal get validation ID` (the definition and its
current evaluations at one prefix); `focal get validation ID --context`
composes, at one prefix, everything an evaluator needs: the claim, the
definition, the registration and evaluation selected like `validation
begin` (`--phase admission|increment`, `--slot N`, `--target ARTIFACT`,
`--generation N`), the target's manifest with each artifact's custody, the
accepted results after `--cursor REVISION` (at most `--limit`) and the
delivery result of the same response; `focal status` prints the standing
read.
Frozen vocabularies (claim status, validation mode) print as their registered
codes. Native-specific flags: `--slot`, `--parent`, `--max-responses` and
`--slot-json` on `submit claim`; `--slot SLOT=ID:HASH` and `--diagnostic
ID:HASH` on `testament submit`; `--slot` on `artifact submit`; `artifact
diagnostic --reason work|production|structure|metadata`; `--validation` and
`--slot` on `validation begin`; `validation report --verdict
pass|fail|incomplete|error`. V1-only fences (`--receipt`, `--evidence-set`,
`--expected-revision`, `--operation PATH`) are refused on a native ledger
rather than ignored. Claim, evaluation and monitor deadlines are logical
milliseconds since the Unix epoch and fire from the node's clock once a
second (a claim expires, an evaluation is fenced, a monitor is settled or
expires) without any command; `get claim` and `list monitors` show the
outcome. Claim batches, graph traversal, validator listing and chunked
uploads are not offered on the native engine; the CLI says so explicitly
instead of answering from the wrong engine. Failed work is
evidence, never an omission: `testament submit --outcome failed` (or any
non-complete outcome) must cite at least one of the holder's own committed
work diagnostics with `--diagnostic ID:HASH` and is refused before sending
without one (exit 2); the claimant reads the diagnostic through `get
artifact` and the testament's `diagnostics` name the exact reference. A check
whose slot the frozen manifest lacks can be neither begun nor reported (exit
4); `validation enter-whole-work TESTAMENT --claim ID` assesses it, ending
the required check and the claim `ValidationIncomplete` without any
manufactured verdict. An evaluator that cannot run its handler reports
`--verdict error`: the error report is retained with its exact target and
attempt, and while the handler's declared `attempts` remain the evaluation
stays open on the next attempt (`attempt_index` counts from zero) for a
further `validation report`; only the final attempt makes it `Errored`. On the native engine `--parent CLAIM_ID` (or `"parent"` in the document) names the committed claim this claim is caused by: the command reads the parent's current binding and receipt and pins them, and the owner admits the child only from the parent's issuer or its current receipt holder while the parent is live, registering the child on the parent; a forged parent is refused before sending (exit 4), a third party is refused as unauthorized (exit 3), and a terminal or changed parent is refused with a typed outcome (exit 5). Cancelling the parent cancels its pending children.

Every remaining owner operation has a verb on a native ledger. Evaluations of
the admission and increment phases are selected on the same `validation
begin` and `validation report` commands with `--phase admission|increment`
(the default is `whole_work`) and, when several increments are current,
`--target WORK_ARTIFACT`; the admission evaluation exists once the claim is
posted, an increment evaluation once the holder submits that artifact. The
issuer's verbs over a cycle are `artifact receive ID --claim ID` (a
generated output), `artifact reject ID --claim ID --reason
structure|metadata --text ERROR_JSON` (the diagnostic inherits the rejected
product's visibility), `validation seal-increments --claim ID` (while the
response is open) and `validation enter-whole-work TESTAMENT --claim ID`
(after receiving it; a plain `validation begin` enters implicitly). The
holder records an unproducible slot with `artifact fail --claim ID --slot N
--diagnostic ID[:HASH]`, citing its own committed `artifact diagnostic
--reason production`. The issuer replaces the holder with `receipt adopt
CLAIM --holder PARTICIPANT|self` (the old receipt is fenced one epoch
earlier; testimony under it is refused as stale), releases a terminal
claim's owned scope with `claim release-scope ID`, and audits a closed claim
with `audit generate --claim ID` then `audit post TESTAMENT` (read with `get
testament`). Durable waits are `monitor register --owner CLAIM --root
satisfied|terminal|released:CLAIM… --at LOGICAL_MS`, `monitor rebind
MONITOR --owner CLAIM --predecessor CLAIM --successor CLAIM` (the successor
must be a committed claim that `supersedes` the predecessor) and, once the
owning claim is terminal, `monitor cancel MONITOR --owner CLAIM`; `list
monitors --claim ID` shows registrations, rebindings and dispositions. Each
verb accepts the same `--json|--yaml|--file` document as its MCP tool
(`focal schema get receipt.adopt --native`); minted identities are reported
under `created` (`Receipt`, `ResultTestament`, `Monitor`, `Artifact`).

```text
focal validation begin --claim <CLAIM> --validation <ADMISSION> --phase admission
focal validation report --claim <CLAIM> --validation <ADMISSION> --phase admission --verdict pass --text '{"passed":1,"failed":0,"skipped":0}'
focal validation begin --claim <CLAIM> --validation <INCREMENT> --phase increment --target <ARTIFACT>
focal artifact receive <ARTIFACT> --claim <CLAIM>
focal validation seal-increments --claim <CLAIM>
focal validation enter-whole-work <TESTAMENT> --claim <CLAIM>
focal claim release-scope <CLAIM>
focal audit generate --claim <CLAIM>
focal audit post <RESULT_TESTAMENT>
focal --client-context alice artifact fail --claim <CLAIM> --slot 1 --diagnostic <DIAGNOSTIC>
focal artifact reject <ARTIFACT> --claim <CLAIM> --reason structure --text '{"code":"malformed","message":"Not a test report."}'
focal receipt adopt <CLAIM> --holder self
focal monitor register --owner <CLAIM> --root satisfied:<OTHER> --at 4102444800000
focal monitor rebind <MONITOR> --owner <CLAIM> --predecessor <OTHER> --successor <SUCCESSOR>
focal monitor cancel <MONITOR> --owner <CLAIM>
```

Lists on a native ledger are bounded scans over the native index families
([22 §7](archictecutre/22-native-record-format.md)). The shared `list`
flags select one indexed predicate and the rest filter within
`--max-visits`, so a page may be empty and still print a `CURSOR`; only a
page without one ends the list, and `--all` follows the continuation for
you. Four families are native-only:

```text
focal list claims --target <ALICE> --status posted
focal list claims --scope file:src/lib.rs
focal list claims --relation reviews=claim:<CLAIM> --created-after 3
focal list artifacts --producer <ALICE> --kind test-report
focal list validations --claim <CLAIM> --evaluator self
focal list evaluations --verdict pass
focal list testaments --claim <CLAIM>
focal list receipts --holder <ALICE>
focal list monitors --claim <CLAIM>
focal list events --after 4:0 --limit 50 --all
```

Claims index issuer (`--source`), subject (`--target`), status, action, one
`--scope`, one `--relation KIND=claim:ID` (or `reviews=artifact:ID`, with an
optional `@HASH` to require one committed hash) and `--created-after`; artifacts
index `--producer`, `--kind`, `--schema-hash` and one `--input KIND:ID`;
validations `--claim` and `--evaluator`; evaluations `--claim`,
`--validation`, `--evaluator` and `--verdict`; receipts `--holder` and
`--claim`; testaments and monitors need `--claim`; events take `--after
SEQUENCE:ORDINAL`. A flag a family does not index (`--caused-by`, `--phase`,
a second `--scope`) is refused rather than ignored. JSON output is the
version 2 result shape with `result.kind = "native_list"`; pass `page.next`
back as `--cursor` in hexadecimal with the same flags. A cursor is bound to
the ledger, principal, route and exact filter and to the node incarnation
that issued it: a tampered, reused or stale cursor is refused.

## Output and recovery

The default output is a compact table. `--format json` and `--format yaml` emit the same versioned structured results with readable top-level IDs, complete typed object/receipt data, read tokens and cursors. YAML is serialized directly to the output sink without making a second whole-result tree. Nested model values retain their frozen wire representation: IDs are byte arrays and vocabularies are numeric codes. `focal schema get domain-registry` provides those codes. Authored input uses readable string IDs and snake-case vocabulary names.

Lists expose `--max-visits` separately from `--limit`: the first bounds examined records, the second bounds returned matches. A page can contain no matches and still carry a continuation. Resume with the same filters and limits. `status` uses the selected client context, including a named remote connection; it does not substitute the local data directory's ledger.

Ordinary mutations use a private managed request stream automatically. The CLI durably reserves an `m1:…` ID and saves normalized input, generated IDs and the exact request before sending. Successful commands print their object outcome without recovery diagnostics. An unresolved command or failed result output prints a copyable recovery command on stderr; a broken diagnostic stream never prevents submission or replaces the original failure. If preparation has not completed, recovery points to pending discovery and explicit sealing rather than retrying an unprepared request. After an abrupt process kill, `focal request pending` discovers its durable reservation or prepared operation even if no output appeared. The data directory must already be private (mode `0700`); newly created node directories satisfy this. An older, publicly searchable directory is rejected rather than silently changing permissions. Its owner can make the selected directory private with `chmod 700 /path/to/data-dir` before using managed requests. Explicit legacy `--operation` journals retain their previous directory requirements. The stream is bound to the selected cluster, ledger and authenticated principal. It does not advance the legacy principal-wide epoch floor. A stream generation issues at most 65,536 IDs; once every one of them is acknowledged, the CLI closes the generation, removes its store and registers the next one on the same slot automatically, without deleting anything by age. IDs of a closed generation report `Retired` from `request inspect` and never execute again. `FOCAL_MANAGED_ROTATION=N` lowers the bound for fault campaigns; the bound is saved with the coordinator on first use, so every later invocation must use the same value.

Several processes of one participant may run at once on the same data directory: CLI invocations beside each other and beside `focal mcp serve`. Ordinary commands read the context catalogue and an enrolled context's credentials under shared locks, so readers never exclude each other; only `context` commands hold them exclusively, and a reader that finds a writer active fails closed rather than waiting. The native request journal is created once under a short creation lock (a creation interrupted before its marker is redone, never reused), and its per-operation lock is waited for briefly instead of failing. A node that refuses a request for capacity (its ingress is full, or the WAL volume is below its free-space watermark) admitted nothing, so the client resends the same request up to three times with backoff and then reports the refusal itself, never an unknown outcome; a journaled native reference refused this way stays `Pending` and commits exactly once on a later `request retry`. A node that died or restarted is noticed by an enrolled client within ten seconds of silence (QUIC keep-alive and idle bound); reconnecting to an endpoint that is still down is bounded by the request timeout. `FOCAL_DISK_HEADROOM_BYTES=N` sets the free bytes the WAL volume must keep before fresh native work is admitted (the standard watermark is 64 MiB; `0` disables it); exact retries of committed work never need headroom.

After a verified receipt, the CLI writes and flushes the result, records delivery durably, and acknowledges only the contiguous prefix of delivered results. Normal use therefore continues beyond the bounded request window without manual cleanup. A timeout, domain refusal, canceled wait or failed output leaves the request recoverable. If cleanup fails after successful output, business success remains success; the next command resumes the saved cleanup. A retired ID cannot execute again and its complete receipt may no longer be available. Successful flush means delivery to the selected output stream, not proof that another application consumed it.

```sh
focal request pending
focal request inspect --operation-id MANAGED_ID --format json
focal request inspect --operation-id MANAGED_ID --remote --format json
focal request retry --operation-id MANAGED_ID --format json
```

Pending discovery includes the bounded CLI and MCP stores, with separate streams so unconsumed MCP results do not fill the CLI window. Inspection does not acknowledge a result. Retrying a committed operation prints its saved result and then records delivery; it never creates a replacement operation. If a refused or never-sent request blocks the window and you intend to abandon that request, `focal request seal --operation-id MANAGED_ID` (also available as `request abandon`) commits an exact request fence or returns its existing committed outcome. Sealing is distinct from canceling the business claim. It does not undo a committed command. `focal request acknowledge --operation-id MANAGED_ID` explicitly confirms consumption of an already saved result, including one produced through MCP.

For automation that needs a known ID before submitting, reserve it first:

```sh
focal request reserve --format json
focal submit claim --operation-id MANAGED_ID --file claim.json
```

Reservation alone sends no business command. If its output is lost, use `request pending` to discover outstanding reservations. Repeating reserve creates another reservation. A caller-supplied `m1:…` ID must already belong to one of these stores; a missing ID cannot create work. Invalid authored input is rejected before default request allocation. A full window caused by unresolved work requires inspecting, retrying or explicitly sealing that work; elapsed time does not make it safe to discard.

Explicit legacy journals retain their original behavior:

```sh
focal submit claim --operation /private/new-operation --file claim.json
focal request inspect /private/new-operation --format json
focal request inspect /private/new-operation --remote --format json
focal request retry /private/new-operation --format json
focal request status --request-id REQUEST_ID --epoch 1 --format json
focal request epoch --epoch 1 --format json
```

`--operation DIR` must name a new directory under an existing parent. It is mutually exclusive with `--operation-id`. Positional inspect/retry arguments always remain paths, even if a filename resembles a managed ID. Different legacy operations use independent request IDs in fixed epoch one; their receipts and journals are not automatically retired. Two processes cannot own one legacy journal concurrently. Preserve these journals for recovery. JSON retains exact Unix `operation_path_bytes`; `operation` is null for a non-UTF-8 path on filesystems that support it. Filesystem rejection returns an IO error before transmission. Existing unqualified 32-hex MCP operation IDs retain their legacy store semantics.

Legacy journal-path `request inspect` defaults to saved local state. With `--remote`, it queries the saved business request at a fresh owner quorum barrier and checks any retained receipt against the exact saved command and existing local receipt. It works even when the journal still awaits epoch admission, and does not modify the journal. `request status` needs only the wire request ID and epoch; `request epoch` observes admission, minimum epoch and latest admitted epoch. The CLI defaults `--epoch` to 1. The authenticated principal and selected ledger always supply the lookup scope; `--source` cannot select someone else's request history.

A retained domain or stream-cursor receipt proves the recorded commit, including when its epoch is below the floor. `BelowFloor` fences new admission at the observed prefix but leaves historical commitment unknown. `Unknown` means no retained outcome is visible and an earlier proposal may still commit. Both are successful observations, not permission to regenerate the command or erase recovery state. Quorum loss returns an operational error. JSON includes the observed domain sequence and applied Raft index, since cursor metadata can commit without advancing the domain sequence. A remote observation never erases a previously saved receipt; use ordinary local inspection to view that receipt and exact retry to persist recovery progress.

| Exit | Meaning |
| --- | --- |
| 0 | Successful read/inspection (including unknown receipt observations), or mutation with a verified receipt |
| 1 | IO, protocol, context or other operational error |
| 2 | Invalid authored input or command syntax |
| 3 | Unauthorized |
| 4 | Object not found |
| 5 | Ambiguous selection, ordinary domain refusal/inform, or retry of a retired managed ID |
| 6 | Operation journal owned by another process |
| 7 | Outcome unknown or missing verified receipt |

Ordinary domain outcomes are also emitted as structured results for mutations. A failed validation remains readable evidence and is not a failed read command. The existing `focal request REQUEST.json` still sends a complete explicit wire envelope; retry the same file after uncertainty.

## Deployment scope

The manual adapter defaults to the local node's identity and ledger. On a joined node it verifies the saved enrollment and uses that Unix listener's actual participant, without taking the running node's storage lease. Named contexts select another local node or an enrolled remote participant. Existing network commands are documented in [network startup](network-startup.md): `start --advertise`, `cluster invite`, and `join`. `deployment explain` is an offline placement plan, not an activated guarantee.

## Errors and exit status

After argument parsing, `--format json` or `--format yaml` also selects the
format of the final diagnostic on stderr. It includes `schema_version`,
`condition`, `error.code`, `error.exit_code` and a bounded message. Stdout remains
the operation result or the pages already delivered. Recovery hints can precede
the final diagnostic on stderr. Syntax errors from the argument parser retain
Clap's standard help text; authored JSON/YAML errors use the selected format.

| Exit | Meaning |
|---|---|
| 0 | Successful read or verified operation result; a recorded failing validation is still a successful submission |
| 1 | Local I/O, transport, invalid response or unrecoverable journal error |
| 2 | Invalid authored input, argument bounds, configuration or unsupported operation |
| 3 | Authentication failure or denied authorization; `unauthenticated` and `unauthorized` are distinct when the transport supplies that distinction |
| 4 | Requested object or saved operation not found |
| 5 | Ambiguous selection, domain outcome, conflicting intent/fence or retired identity |
| 6 | Busy, server capacity, temporarily unavailable authority or incomplete bounded selection/transfer |
| 7 | Unknown mutation outcome; recover the original saved operation |
| 8 | Expired read snapshot or retention gap; explicitly start a new query or resynchronize |
| 9 | Offline placement cannot satisfy the requested durability guarantee |
| 130 | Interrupted client wait or a canceled upload |

The code describes the current failure; it does not prove noncommit of an earlier
request. `Inform`/wait outcomes retain their domain result and saved identity.
Receipt retirement, upload cancellation and a canceled client wait remain
different operations. The shared Rust failure classification is also used by
MCP, including nested request-store, upload, watch and administrative errors.

## Select a client context

```sh
focal context add work --node-data-dir /absolute/path/to/node
focal context use work
focal list claims
focal --client-context work get validation VALIDATION_ID --context
focal context show
focal context list
focal context use local
```

To enroll an independent remote participant, create a client invitation on the running founder, then redeem it in the client's own directory:

```sh
focal --data-dir /node cluster client invite --name alice --output /private/alice.invite
focal --data-dir /client context enroll alice --invite-file /private/alice.invite
focal --data-dir /client context use alice
focal --data-dir /client list claims
focal --data-dir /client mcp serve
```

Retry `context enroll` with the same name and invitation after interruption. It preserves the original key, CSR and enrollment request. Context selection applies to domain operations and MCP. `context add NAME --file FILE` also accepts a strict JSON/YAML connection document for existing DER credentials. `context show` redacts credentials. Removing a context retains its request history; reusing that name for a different identity is rejected. Missing initialized context or history files require restoring those files.

See [the implementation contract](archictecutre/19-cli-mcp-implementation.md) for the peer lifecycle operations, scoped protocol admission, and separation between command availability and the independent lifecycle storage migration.

The full command inventory, cluster progression, MCP/skills mapping and challenge/consult policy remain required in [the source research](archictecutre/11-cli-spec-research.md), [agent workflows](archictecutre/12-agent-tools-and-workflows.md), and [P17–P20](archictecutre/13-cli-and-agent-implementation-plan.md). The local [MCP adapter](mcp.md) is available. Complete cluster administration and global-scale qualification remain open.


### Inspecting external validator contracts

Use `focal validator list` to inspect handlers pinned by recorded validation
requirements. It needs no filter. Narrow the page with `--claim`, `--evaluator`,
`--kind`, `--phase`, `--mode`, `--agentic true|false`, or `--schema-hash`.

```sh
focal validator list --claim CLAIM_ID
focal validator get HANDLER_ID --version VERSION_HASH --format json
```

The result preserves each requirement's evaluator, full handler chain, quality
bar, evidence schemas and policy revision. The same handler version can appear
under different requirements. Focal reports those bindings; the participant
supplies and invokes its own implementation. Neither command loads code or
checks whether a program is installed in another participant's environment.

`--limit` bounds returned requirements and `--max-visits` bounds search work.
Preserve all filters and limits with a returned `--cursor`. A filtered page may
be empty and still have a continuation. An empty final page means no further
recorded bindings at that prefix. JSON output retains the exact requirements and
read token; table output shows the matching handlers.


### Atomic claim batches

`focal submit claims --file batch.yaml` generates the batch under one durable
operation reference. The document is `{"claims":[...claim documents...]}`;
`--json` and `--yaml` accept that same shape. Alternatively, repeat
`--claim-json` or `--claim-file` to supply complete individual claims. File
entries precede inline entries when both repeated options are used. Document
input and repeated field inputs cannot be combined.

A batch contains 1–64 claims within the aggregate authored input bound. Assign
explicit claim IDs when members refer to one another through dependencies.
Each member still needs its own immutable validation requirements. Admission is
atomic: an invalid member cannot leave earlier members committed. Generated
claims are posted separately using `claim post`.


## Watch delivered ledger changes

```sh
focal watch claims
focal watch artifacts --claim CLAIM_ID --name evidence
focal watch all --name changes --format json
focal watch inspect --format json
focal watch resume evidence
```

`claims`, `testaments`, `artifacts`, `validations` and `all` are available. Omit `--claim` for the complete ledger; repeat it for a union of claim associations. Defaults seed the current graph at one fixed prefix and then follow the retained delta tail. `--no-seed` starts from available retained history. `--limit 1..256` bounds source visits/items; `--pages N` stops after N flushed pages. JSON output is one complete delivery per line; YAML output uses one `---` document per delivery. Preserve the selected `--data-dir` and `--client-context` on recovery.

Each named watch saves its exact pending request or one unconsumed page before output. CLI output is acknowledged locally only after write and flush succeed; the next source poll commits the cursor acknowledgment. Ctrl-C or a broken output pipe retains unfinished delivery and prints a copyable `watch resume` command. A crash after flush but before the durable local acknowledgment may replay the same delivery ID: sinks should deduplicate that ID. Stopping after `--pages` can leave completion maintenance for the next resume. Inspecting never consumes a page.

Seed pages contain real graph objects. Claim-filtered seeds use the committed association index, not artifact provenance; empty filtered pages still advance. Each seed continuation enforces a 64-KiB page bound at the source. An individual row that cannot fit returns capacity rather than skipping it. The original seed lease must survive until every page is consumed. Expiry or server restart during a partial seed fails explicitly; choose a new watch name and reseed after deciding how the sink handles overlap. A retained page remains recoverable even when its source lease has expired.

Tail output contains the original delta and resolved/resync markers. Family selection retains its recorded facts; validation watches also retain original claim-generation facts because those commits create their pinned requirements. These are not synthesized independent artifact/testament lifecycle events. The future independent lifecycle history remains a separate storage upgrade.

On a native ledger the same commands run unchanged. The watch speaks the native wire profile, seeds through linearizable native reads after the source pins the snapshot instead of a server-side snapshot scan (a `--claim` filter reads each claim with its responses and evaluations; `claims`, `testaments` and `all` without a filter list every claim, `artifacts` lists the artifacts and `validations` the definitions), and then follows the tail of schema-2 deltas derived from the committed native records. Each seed page is a `NativeSeed` delivery (`token` names the native prefix the objects were read at, `next` the following step); each tail delta carries `schema: 2`, a `Native` fact with the exact committed event (`sequence`/`ordinal` are the native record position), the nearest legacy `action`, the `actor` (zero for trusted timers and the import) and the `claim`. Table output prints native changes as `CHANGE <sequence> <action> <claim> native:<record>.<ordinal> <fact>`. Facts committed between the snapshot and the seed's read prefix appear in the seed and again as deltas; deduplicate by object binding. The engine a watch was created for is saved with its options, so a name keeps its engine across resumes; watch journals written by earlier development builds are refused.

There are at most 16 saved watch names per selected context, each with an independent four-slot managed cursor stream and bounded journal. CLI and MCP can inspect/resume the same names, while ordinary mutation streams remain separate. Names/options are immutable; use `--name` for a distinct watch. Preserve `WATCHES.watch-owner`, `WATCHES.watch-lock`, the `watch-*.watch-*` files and their adjacent managed `.requests` state together. Automatic watch deletion, slot rotation and expired-cursor repair are not implemented; deleting initialized files is not recovery.

## Validate authored input and review a raw request

Use the same bounded JSON/YAML documents as ordinary commands:

```sh
focal schema validate claim.submit --file claim.yaml --shape-only
focal --data-dir ./node schema validate claim.submit --file claim.yaml
```

`--shape-only` checks strict DTO decoding, duplicate/unknown fields and input
structure limits without loading settings or a client identity. It explicitly
reports that identity and domain semantics were not checked. Without it, Focal
loads the selected client context and runs the shared authored builder's local
preflight. A missing, corrupt or explicitly selected unknown context is an error;
it never silently falls back to shape-only validation. No request, operation
reservation, generated identity or mutation journal is published by validation.
Actual authority, lifecycle state, evidence custody and server acceptance still
require the running owner. Use `schema example claim.submit` for a complete
starting document.

Normal submissions remain one command with automatic managed recovery. The
following expert workflow instead creates a **legacy raw request** whose request
epoch and identity you manage:

```sh
focal --data-dir ./node request build claim.submit --file claim.yaml \
  --request-epoch 1 --output claim.request.json
focal request check claim.request.json
focal --data-dir ./node request send claim.request.json
# After an unknown reply or process restart, send precisely the same file:
focal --data-dir ./node request claim.request.json
```

`build` performs no network call. It expands authored IDs once and publishes the
complete JSON envelope through a private mode-0600 temporary file, file fsync,
atomic no-clobber link and directory fsync. Existing paths, including symlinks,
are refused. Keep the output file when an output/flush error follows publication.
The printed BLAKE3 hash covers the postcard wire envelope, including its complete
request key. `--request-id` accepts an explicit nonzero 32-hex ID; omitted IDs are
generated once. Mutations require `--request-epoch`, and **that epoch must already
be admitted for the authenticated principal** before sending; this raw workflow
does not negotiate epochs, register a managed stream, or create an `m1:` ID.
Read-only operations default to epoch 1. Revision-fenced participant operations
also require an explicitly observed `--expected-revision`; offline build cannot
fetch one. Composed operations such as `validation.context` and filtered singular
`claim.get` cannot be represented by one raw envelope and are rejected.

`check` reads a strict complete JSON envelope (256 KiB, bounded depth/node count),
checks shared wire syntax, protocol shape, ledger consistency and resource limits,
and prints the same hash. It cannot prove authentication, current authority,
legacy epoch admission, lifecycle or custody state, or server acceptance. Managed
ownership/control and node/control peer families require their dedicated
workflows and are explicitly unsupported by this offline checker. The generated
JSON file is also bounded to 256 KiB; wire encoding is independently checked
against the default 1-MiB frame limit. `request send FILE` is an alias for the
existing positional raw sender, retaining its 1-MiB input limit and wire surface.
It never rebuilds IDs or silently converts the request into managed work.

## Stream every list page

```sh
focal list claims --all --limit 64 --max-visits 256 --format json
focal list artifacts --all --kind test-report --format yaml
```

`--all` works for all four list families. It follows the server's continuation
with the same filters, item/visit limits and exact read prefix; an empty matching
page is still progress when its cursor is present. JSON is one complete page per
line, with the same fields as ordinary list output. YAML uses a separate `---`
document for each page. Table output includes each page's prefix and continuation.
Without `--all`, the existing one-page behavior is unchanged.

Only one source page and its encoded output buffer are retained at a time.
The encoded page is capped at 16 MiB, including YAML expansion. The next request
starts after the page has been fully written and flushed. Ctrl-C, broken output,
capacity failure, or an expired read lease stops with an error and identifies
already flushed pages as a partial result. Keep their prefixes and use the
reported cursor with the same filters and limits while that lease remains valid;
a partially written final document must be discarded or deduplicated on replay.
Restarting without its cursor is a new read, not continuation of the old prefix.
MCP list tools remain individually paginated rather than accumulating all results
inside one tool call.

## Document input for lifecycle verbs

The existing positional/flag syntax remains supported. These scalar verbs also
accept exactly one `--json`, `--yaml`, `--file`, or stdin document, using the same
authored DTO as their MCP operation:

| CLI verb | Authored fields |
|---|---|
| `claim post` | `claim` |
| `claim progress` | `claim`, `receipt: {id, epoch}`, `message` |
| `claim cancel` | `claim`, `reason` |
| `receipt acquire` | `claim`, `epoch`, optional new receipt `id` |
| `evidence begin` | `claim`, `receipt: {id, epoch}`, optional evidence-set `id` |
| `testament receive` | `claim`, `testament` |
| `validation begin` / `validation complete` | `claim` |

For example, `focal claim post --file post.yaml` accepts `claim: 'CLAIM_ID'` after
replacing the placeholder with the actual ID. Use `schema get claim.post` or the
corresponding operation name for the complete strict schema. Document fields
cannot be combined with positional domain IDs or domain field flags; output,
operation recovery and expected-revision options remain envelope options. The
flag form `receipt acquire CLAIM_ID` retains its default epoch 1; documents name
that epoch explicitly. All forms pass through the same builder and durable
preparation path, preserving current authority, receipt and revision checks.

List predicates also cover immutable scope, lineage, provenance, testament
outcome, and creation prefix. All are optional and conjunctive:

```sh
focal list claims --scope 'file:src/parser.rs' --relation 'issuer=participant:self' --all
focal list claims --caused-by root:00000000000000000000000000000001 --created-after 100 --created-through 200
focal list artifacts --input claim:00000000000000000000000000000002
focal list testaments --outcome complete --confidence committed
focal list validations --created-after 100
```

Repeat `--scope`, `--relation`, or `--input` to require every supplied predicate
(up to 16 of each; `--caused-by` counts toward the relation bound). Relation
queries use `KIND=TYPE:TARGET`: object types are `claim`, `testament`, `artifact`,
and `validation`; other targets are `participant:self|ID`, `root:ID`, and
`action:ACTION`. This typed query syntax does not change authored claim relation
syntax. Scopes and relations apply to claims, inputs to artifacts, and
outcome/confidence to testaments. Unsupported family combinations fail before
transmission.

`--created-after` is exclusive and `--created-through` inclusive; both use
committed creation `SessionSeq`, not wall time or latest activity. These bounds
apply to all four families. Generic change/activity and validation-result list
predicates are not available; inspect validation runs with `get validation` or
`validation context`. Claim scope, relation/cause and creation predicates also work with singular
`get claim`, which proves uniqueness at one prefix and reports ambiguity rather
than choosing a match.

Pages remain bounded by result count, visited records, and bytes. A page may
contain no matches and still have a continuation. Keep predicates and page
limits unchanged when using its cursor; `--all` follows this rule automatically.
A cursor retains one principal and exact snapshot. Expired or restarted-server
cursors fail explicitly; start a new query to observe a new prefix.

## Observe a claim until a condition holds

```sh
focal claim wait CLAIM_ID --until satisfied --timeout-ms 5000
focal claim wait CLAIM_ID --until terminal --format json
focal claim wait --file wait.yaml --format yaml
```

Replace `CLAIM_ID` with the actual ID. The document form is
`{ "claim": "CLAIM_ID", "until": "satisfied", "timeout_ms": 5000 }`;
`until` accepts `satisfied`, `terminal`, or `released`, and on a native ledger
also `testament` (the issuer has received a closing testament). `timeout_ms`
defaults to 30000 and accepts 1–30000. The MCP tool `claim.wait` takes this
same document and has no `operation_id` or reservation step.

The observer performs fresh quorum claim reads, keeping only the latest observed
status, revision, local-completion flag, release flag and read token. It makes at
most 31 probes, waits at least one second between completed probes, and shares
one deadline bounded by the selected client's retry deadline and 30 seconds.
For longer observation use a durable named `watch`; repeat waits do not retain a
read cursor or subscribe to historical state changes.

| Condition | Meaning | CLI exit |
|---|---|---|
| `Met` | The observed committed state meets the requested predicate. | 0 |
| `Pending` | The observer deadline elapsed after a successful observation. | 6 |
| `Unmet` | A satisfaction wait observed a terminal non-satisfied claim. | 6 |

JSON/YAML output includes the actual observation under `result.result`, with
`result.kind` equal to `claim_wait`, and the condition at the top level. Pending
and Unmet are MCP read results (`isError: false`), not mutation receipts or
business failures. The CLI flushes that result before returning its exit code.
A missing claim, invalid response, or transport failure remains a typed error;
a timeout before any successful observation cannot invent a Pending result.
Ctrl-C or MCP cancellation stops only this observer. No monitor, timer, claim,
work execution or operation journal is created, and no lifecycle state changes.
Terminality alone does not imply release; a release wait checks the actual flag.
