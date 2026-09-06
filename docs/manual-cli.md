# Manual CLI

The `focal` binary connects to the running local service through its authenticated Unix socket. Start it in one terminal:

```sh
focal --data-dir /tmp/focal-manual start
```

Use the same `--data-dir` in another terminal. The examples below abbreviate that common option. `--config FILE` and `--data-dir DIR` work before or after subcommands. `--help` lists the flags at each level. Build with `bash scripts/cargo.sh build -p focal-node --bin focal --locked --offline`; the development binary is `target/debug/focal`.

## Submit a claim

Each claim needs an explicit acceptance contract, including a required whole-work receipt validation. `--target` names the subject participant. IDs are 32 hexadecimal characters; hashes are 64. `self` resolves only the authenticated local participant and is accepted in participant fields. The domain prohibits self-targeted `work`; the following local handoff uses the explicitly permitted `handoff` action.

```sh
focal submit claim \
  --target self --action handoff \
  --description 'Deliver the checked report' \
  --scope file:report.json \
  --validation-json '{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}'
```

The result includes the generated claim ID and a durable operation directory. This generates the claim; `focal claim post CLAIM_ID` makes it actionable. A receipt requirement checks receipt of the testament. Substantive work acceptance requires additional pinned validation definitions. The challenge/consult orchestration and automatic corrective/follow-up issuance remain planned in [P20](archictecutre/13-cli-and-agent-implementation-plan.md).

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

Each fresh invocation generates a new occurrence and request identity. To retry an earlier submission, use its operation directory. Supplying the same explicit IDs and authored fields yields the same canonical command across flags, JSON and YAML; it does not replace the durable request identity.

Choose field flags or one document. Documents cannot override issuer, trusted cause, runtime authority, lifecycle or custody. Unknown fields, duplicate keys, YAML aliases/tags/multiple documents, numeric vocabulary codes, excessive nesting and over-budget input are rejected. A document is limited to 256 KiB. Validation definitions can also be supplied with repeated `--validation-file FILE`; aggregate definition bytes are bounded. Detailed authored fields are defined by [the shared DTOs](../crates/focal-client/src/input/documents.rs).

## Deliver artifacts and a testament

Use the IDs printed by each preceding step:

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

Artifacts accept the same document modes. An inline payload document is `{type: text, text: ...}` or `{type: inline, bytes: [...]}`; a content payload contains an existing immutable content reference. Inline payloads and opaque metadata are each limited to 16 KiB. `--payload-file FILE` supplies inline bytes. This release's service admission verifies the built-in test-report schema; arbitrary schema registration and the convenient resumable upload command remain open. The low-level upload protocol is available through `focal request FILE`.

Closing the testament records `TestamentGenerated`. It does not by itself acknowledge the testament, run validators or establish satisfaction. The embedded `focal demo` exercises the real validator flow under exclusive ownership; the foreground service's automatic worker orchestration remains separate work. `focal claim cancel CLAIM_ID --reason TEXT` commits business cancellation subject to domain standing and lifecycle rules.

## Get and list

```sh
focal get claim CLAIM_ID
focal get claim --source PARTICIPANT_ID --target PARTICIPANT_ID
focal get testament TESTAMENT_ID
focal get artifact ARTIFACT_ID
focal get artifact ARTIFACT_ID --output ./report.json
focal get validation VALIDATION_ID

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
| Claims | `--claim`, `--source`/`--issuer`, `--target`/`--subject`, `--status`, `--action` |
| Testaments | `--claim` |
| Artifacts | `--claim`, `--testament`, `--producer`, `--kind`, `--schema-hash` |
| Validations | `--claim`, `--evaluator`, `--kind`, `--phase`, `--mode` |

`list artifacts --testament` follows the immutable manifest, so a related artifact outside that manifest is excluded. `get artifact --output FILE` streams content in at most 64 KiB pages and atomically publishes a new private file after complete retrieval and fsync. Existing files, directories and symlinks are never overwritten. The service verifies the addressed manifest and each stored chunk; the CLI checks exact offsets, length and EOF. The manifest root is not a digest of concatenated raw bytes. Independent client-side manifest proof export remains open. A publication fsync failure may leave a complete output file while returning an error; it never reports success for partial content.

Lists return at most 100 matches by default; `--limit` accepts 1–256. Each service page also bounds records visited and encoded bytes. Copy the returned `CURSOR` into `--cursor`, retaining the same filters. A page with no matches may still have a cursor. Server list cursors bind authentication scope, query and exact prefix, and expire with snapshot retention. An expired or invalidated prefix requires starting a new read.

`get validation` returns the requirement plus bounded run summaries and committed verdict attempts, including evaluator, handler, manifest, target, epoch, attempt and evidence references in JSON. Its `--limit` and `--cursor` page through that one requirement at the same prefix. A known requirement with no execution has an empty records array; a missing requirement returns not found. These reads do not start validation work.

## Output and recovery

The default output is a compact table. `--format json` emits versioned structured results with readable top-level IDs, complete typed object/receipt data, read tokens and cursors. Nested model values retain their frozen wire representation: IDs are byte arrays and vocabularies are numeric codes. `focal schema get domain-registry` provides those codes. Authored input uses readable string IDs and snake-case vocabulary names.

Every manual mutation saves an exclusive, checksummed, private operation journal before its first network attempt. Its generated IDs, command bytes, selected principal/ledger/cluster and request epoch are fixed. The journal remains pending after a timeout, denial or inconclusive reply; only a verified durable receipt advances it.

```sh
focal request inspect /path/to/operation --format json
focal request inspect /path/to/operation --remote --format json
focal request status --request-id REQUEST_ID --epoch 1 --format json
focal request epoch --epoch 1 --format json
focal request retry /path/to/operation --format json
```

An explicit `--operation DIR` must name a new directory under an existing parent. Omit it to use a generated directory under the node's `client/operations`. Two processes cannot own one journal concurrently. Different operations safely use independent request IDs in fixed epoch one; automatic epoch-floor advancement and journal garbage collection remain open. Preserve journals needed for recovery. Inspect/retry never recreate missing operation state. JSON includes exact Unix `operation_path_bytes`; `operation` is null for a non-UTF-8 path on filesystems that permit such names. Filesystem rejection, including macOS rejecting invalid UTF-8 names, returns an IO error before transmission. Diagnostics and the recovery reference go to stderr; structured results go to stdout.

`request inspect` defaults to saved local state. With `--remote`, it queries the saved business request at a fresh owner quorum barrier and checks any retained receipt against the exact saved command and existing local receipt. It works even when the journal still awaits epoch admission, and does not modify the journal. `request status` needs only the wire request ID and epoch; `request epoch` observes admission, minimum epoch and latest admitted epoch. The CLI defaults `--epoch` to 1. The authenticated principal and selected ledger always supply the lookup scope; `--source` cannot select someone else's request history.

A retained domain or stream-cursor receipt proves the recorded commit, including when its epoch is below the floor. `BelowFloor` fences new admission at the observed prefix but leaves historical commitment unknown. `Unknown` means no retained outcome is visible and an earlier proposal may still commit. Both are successful observations, not permission to regenerate the command or erase recovery state. Quorum loss returns an operational error. JSON includes the observed domain sequence and applied Raft index, since cursor metadata can commit without advancing the domain sequence. A remote observation never erases a previously saved receipt; use ordinary local inspection to view that receipt and exact retry to persist recovery progress.

| Exit | Meaning |
| --- | --- |
| 0 | Successful read/inspection (including unknown receipt observations), or mutation with a verified receipt |
| 1 | IO, protocol, context or other operational error |
| 2 | Invalid authored input or command syntax |
| 3 | Unauthorized |
| 4 | Object not found |
| 5 | Ambiguous selection or ordinary domain refusal/inform |
| 6 | Operation journal owned by another process |
| 7 | Outcome unknown or missing verified receipt |

Ordinary domain outcomes are also emitted as structured results for mutations. A failed validation remains readable evidence and is not a failed read command. The existing `focal request REQUEST.json` still sends a complete explicit wire envelope; retry the same file after uncertainty.

## Deployment scope

The manual adapter currently selects the laptop/founder's local identity and ledger. Joined-node markers cause an explicit error before transmission because that node's Unix principal differs from the founder metadata; remote authenticated client contexts and ledger selection are still required. Existing network commands are documented in [network startup](network-startup.md): `start --advertise`, `cluster invite`, and `join`. `deployment explain` is an offline placement plan, not an activated guarantee.

The full command inventory, cluster progression, MCP/skills mapping and challenge/consult policy remain required in [the source research](archictecutre/11-cli-spec-research.md), [agent workflows](archictecutre/12-agent-tools-and-workflows.md), and [P17–P20](archictecutre/13-cli-and-agent-implementation-plan.md). The local [MCP adapter](mcp.md) is available. Complete cluster administration and global-scale qualification remain open.
