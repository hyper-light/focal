# Peer validation and independent object lifecycles

Status: implementation contract revised by the user's 2026-09-06 steering.
This document identifies required changes; it does not claim the new lifecycle
records are already implemented. The existing-profile peer APIs are recorded in
[19](19-cli-mcp-implementation.md); they do not supply the independent states
specified here. This contract refines
[02](02-domain-and-lifecycle.md), [12](12-agent-tools-and-workflows.md), and the
P02/P05/P17–P20 work in [13](13-cli-and-agent-implementation-plan.md).
The precise target states, writers, aggregation choices and forbidden transitions
are now fixed in [17](17-lifecycle-state-and-authority.md). The required format
and decoder transition is specified in
[18](18-lifecycle-storage-upgrade.md). These target rules supersede conflicting
future-facing lifecycle prose; existing committed records still use their original
format and reducer semantics.

## 1. The boundary the user requires

Focal is a peer-to-peer claims ledger, not a job system. It does not launch agents,
allocate model workers, load scripts into the ledger process, or invoke an
arbitrary tool supplied in claim content. Participants supply their own execution
environments. The participant issuing a claim invokes validating tools, skills,
scripts, or code in that environment. A designated evaluator may be another
participant; consultation with that peer uses ordinary claims and testaments.

Focal records requirements, evidence, lifecycle facts, and authenticated evaluation
results. It checks whether a requested transition is authorized and consistent
with those facts. Durable validation run identities and phases are evidence and
concurrency fences, not leased jobs. Committed deltas notify participants of work
they may need to perform; delivery of a delta does not perform that work.

The optional `focal-runtime` crate currently contains an embedding helper with
validator execution threads. That is existing participant-side implementation
machinery, not a requirement that the Focal daemon become an agent launcher. Its
current implementation and tests must not be confused with a completed public
peer evaluation interface. The foreground CLI service currently does not install
that evaluator helper automatically.

The implementation language is Rust. Public integration contracts are language
agnostic, with versioned schemas and authenticated operations. Native Rust types,
CLI flags, JSON, YAML, MCP tools, and other language clients express the same
operations. YAML is an optional authored representation, not an execution language
or a prerequisite for programmatic validation.

## 2. Source evidence and the correction to the current model

The primary detailed sources are Sylk's
[Artifacts and Validations](../../../sylk/docs/ARTIFACTS_AND_VALIDATIONS.md) §§2,
5–7, 9 and 11, and
[Claims and Testaments Lifecycle](../../../sylk/docs/CLAIMS_AND_TESTAMENTS_LIFECYCLE.md)
§§3.3, 5–6 and 8. Hecate preserves the distinction between delivery and quality,
streamed evidence and closing testimony, and immutable content versus lifecycle in
[its ledger architecture](reference/hecate/docs/architecture/LEDGER.md).

Sylk explicitly specifies separate object lifecycles and transition owners.
Artifacts stream into durable storage before a testament closes the work cycle.
The claimant may acknowledge an artifact during its unattached window. The
testament's later receipt is a different fact. Validations on an artifact can run
concurrently; programmatic and quality phases within one validation remain ordered.
Triggered child and parent transitions are committed atomically.

Current Focal has a full claim status vocabulary, receipt-fenced evidence staging,
immutable manifests, and validation runs with attempts. However,
[`objects.rs`](../../crates/focal-model/src/objects.rs) currently represents:

| Family | Existing lifecycle record | Missing contract |
|---|---|---|
| Claim | Status, history, execution receipt, active evidence/testament and graph release | Integration with independent artifact/testament facts and peer-authorized propagation |
| Testament | Creation sequence and optional acknowledgment sequence | Distinct activation, receipt, evaluation, failure and success history |
| Artifact | Creation sequence and custody revision | Generated/received/attached/evaluating/terminal history and transition ownership |
| Validation | Creation sequence and latest run epoch, plus separate run/attempt records | Explicit single-target binding and independently inspectable phase/terminal history |

Those reduced records are an implementation gap. A CLI label or a status inferred
from the claim cannot supply the missing durable facts. Current stored artifacts
are visible before close, but storage custody is not claimant receipt, and an
artifact's presence in a manifest is not proof that it passed a validation.

Existing Focal decisions remain explicit. D-06 permits any participant category
to issue a quality requirement with a designated evaluator and declared agentic
execution contract. The ordinary authorization is an authenticated Actor whose
principal matches that committed designation; a global Evaluator role registry is
not required. Additional capability or delegation checks apply only when the
claim's applicable policy explicitly requires them.
This supersedes Sylk's category restriction. Focal also retains its graph
satisfaction predicates, immutable historical identities, and versioned decoding.
Sylk's “no versioning” does not authorize changing committed Focal encodings.

## 3. Four state machines with explicit synchronization

### 3.1 Claim: the obligation

Keep generation distinct from posting, claimant distinct from subject, and
execution receipt distinct from observed delivery. Progress is observational.
Closing a response does not satisfy the obligation. Required evaluation results
and declared graph predicates determine satisfaction. Cancellation, expiry,
supersession and other terminal controls fence further work according to the
existing model; immutable terminal history is preserved.

A claim's status is its own lifecycle projection. It is not a universal status
field for every artifact, response and evaluation associated with that claim.

### 3.2 Artifact: independently visible evidence

The target lifecycle follows Sylk's recorded paths:

`generated → [received] → attached → validating → validated`

Generation failure, structural receipt failure, and required validation failure
are separate terminal outcomes. The optional `received` step records claimant
observation before attachment; it is neither consumption nor validation. If the
testament closes first, `generated → attached` is valid. Do not fabricate a
historical receipt event to fill that gap.

The testifier commits generation. The claimant commits receipt or a supported
structural receipt failure. The testifier binds the artifact to its testament at
close. The claimant or explicitly designated evaluator records evaluation progress
and results under the declared authorization. No one party owns every transition.

Artifact creation must be a committed, visible fact before testament creation.
The closing transaction freezes the manifest and records attachment together.
Artifact bytes, immutable metadata, schemas and content hashes are never rewritten
to represent lifecycle progress. Durability/custody revisions remain a separate
storage concern.

### 3.3 Testament: the closing response

Represent generation, posting, receipt, validating, validated, incomplete, failed
and errored as distinct recorded lifecycle facts. Generation freezes the response
and its artifact bindings; posting activates that response for the claimant;
receipt is committed by the claimant. The respondent must author a testament
after its work finishes or fails; receipt of the claim does not create one. A
failed or partial response uses the same testament mechanism, with explicit
summary, confidence and reported outcome, and must include real error artifacts.
Diagnostics remain inspectable when the requested work artifact is absent.
Native diagnostic references are separate from work-slot bindings: reporting an
error does not supply an otherwise missing output or prove a requirement passed.
The first response's generation advances the claim to TestamentGenerated in the
same commit as its manifest attachment. Posting changes the testament's own state;
it does not defer that already-durable claim fact or invent a TestamentPosted claim
status. A generated but unposted response still cannot be received or evaluated.

An ordinary testament closes a work cycle. Its existence does not imply every
artifact passed, that the claimant has received it, or that the claim is terminal.
The current single-active-close-per-receipt rule in D-03 must be reconciled
explicitly with independent testament state. Supporting more than one response
cycle requires named lineage and aggregate rules; it must not be implemented by
overwriting the active testament or silently merging manifests.

[17 §5](17-lifecycle-state-and-authority.md#5-response-testament-transitions-and-multiple-work-cycles)
resolves this for the new profile: one response remains the default; additional
cycles are bounded and explicitly related. Already-posted eligible success
witnesses are considered before an atomic claim failure, never afterward to reopen
a terminal claim. Each response retains its own manifest and outcome.

Validator result evidence has a distinct role. Sylk's claimant-issued result
testament terminates at posting, and its result artifacts terminate at generation.
They provide an audit trail without creating another response/validation cycle.
Focal's current single main-testament pointer and receipt/evidence-set-bound
`TestamentContent` cannot represent this role. Introduce explicit role and relation
types, exclude result evidence from ordinary completion aggregates, and never
replace the respondent's main testament with the claimant's result bundle.

### 3.4 Validation: one declared check and its results

Keep the immutable requirement separate from its execution facts. Bind a run to
one exact declared target, the validator contract/version, evaluator, receipt
generation when applicable, phase, and attempt. For artifact checks, resolve the
declared artifact slot/name to one immutable artifact ID and digest. A shared
schema alone cannot choose arbitrarily between two artifacts of that schema.

The normal progression is ready → validating → validated or a typed terminal
outcome. When both phases are declared, quality follows a successful programmatic
phase. A declared agentic-only check enters its agentic phase directly, with no
fabricated programmatic Pass. Structural admission and authority checks still apply
to every check; they are not an external programmatic evaluation.
Failure, missing evidence, and execution error remain different outcomes. Required
versus Observe controls propagation; it does not erase an optional failed result.

Different requirements progress independently and can be evaluated concurrently by
participants. They do not become one serial chain merely because they belong to
the same artifact or claim. Ordered fallback and programmatic-then-quality phases
are internal to a particular requirement. An unavailable tool cannot be recorded
as evidence that the respondent's work failed.

Existing admission and increment phases retain explicit target and readiness rules.
They do not permit ordinary whole-work artifact validation merely because an
unattached artifact became visible. Sylk's whole-work path waits for attachment and
the claimant's evaluation transition.

### 3.5 Coupling and terminal races

| Committed fact | Permitted dependent effect |
|---|---|
| Artifact generation | Visibility; claimant may acknowledge observation |
| Testament generation/close | Exact manifest attachment and first-response claim TestamentGenerated in the same transaction |
| Testament receipt | Response-delivery fact; pure receipt requirements may pass |
| Evaluation begins for an attached artifact | Eligible checks enter their own evaluation phase |
| Last required check passes | Artifact may validate; eligible testament/claim aggregation follows |
| Required check fails or lacks evidence | Record the exact cause; propagate the appropriate failure when its aggregate conditions hold |
| Optional check fails | Preserve its result without failing required work |
| Validation result artifact is recorded | Audit evidence; no recursive validation obligation is implied |

Every propagation request must be checked against committed/effective state inside
the owner and committed atomically with its triggering facts. Participants cannot
submit an arbitrary parent status or manufacture satisfaction. Focal may compute
or verify the deterministic consequences of the authorized mutation; it does not
invoke the validator to produce those facts.

The new profile's short-circuit decision is fixed in
[17 §7](17-lifecycle-state-and-authority.md#7-aggregation-short-circuit-and-races):
first committed blocking cause, with valid alternative response witnesses checked
before claim terminality. Unbegun siblings become ineligible, while already-begun
checks may complete their pinned phases for audit under live authority. Late
accepted evidence never repaints a terminal parent. Receipt adoption, cancellation
and stale evaluator generations still take precedence over an external response
that no longer has authority. Today's all-required-final aggregation and
Fail > Error > Incomplete precedence remain exact for its legacy profile.

Result audit closure uses the bounded begun-check cohort, not an impossible wait
for short-circuited Ready checks to produce verdicts. Explicit suppressed/fenced
records preserve why no result exists. Neither closing the audit bundle nor a late
Observe result is another response or a recursive validation trigger.

## 4. Supplying and invoking validators

A validator reference identifies a check, not a participant's deployment. A
participant may bind it to a tool, skill, script, executable, or native function in
its own environment. The claim records the immutable contract necessary to know
what was required: reference identity/version, input and result schemas, target,
instructions or rubric, mode and evaluator. Mutable endpoint addresses, credentials,
worker pools and process placement do not belong in authored claim content.

The existing `HandlerRef { id, version, agentic }` is useful as an identity fence.
It currently does not retain an external locator or definition artifact. Extending
the authored descriptor must preserve those references explicitly. Use a bounded,
immutable definition artifact with a typed requirement binding, or a versioned
requirement field. Do not discard the reference or hide executable authority in
free-form description text.

Native Rust authors can implement the existing `focal_evidence::Validator` trait
or invoke code from their own participant application. Other languages use the
same public operations and result schemas. Programmatic execution remains code;
an optional JSON/YAML definition describes its contract. Skills describe agentic
behavior and required tools; a skill's text is not a verdict. Tool output must be
captured as evidence and submitted by the designated evaluator.

Authentication establishes who submitted a result. It does not establish that an
arbitrary external program ran exactly as claimed. Preserve the invoking
participant, pinned validator reference and result evidence as provenance. Any
requirement for independently verifiable execution must be an explicit evidence
contract; Focal must not silently describe an issuer's report as independent
attestation.
The declared programmatic/agentic execution kind identifies the evaluation contract,
not a globally assigned security role. A laptop participant can designate its own
authenticated Actor identity, invoke its custom tool or skill externally, and use
the planned narrow begin/result operations without registering a worker or gaining
Runtime authority. Focal checks the principal, committed requirement, evidence and
fences. A policy that additionally requires an approved evaluator or attestation
must state and enforce that requirement explicitly.

A validator reference also does not grant authority to perform the underlying
work. Deployments, resource changes and other work-producing effects require an
explicit claim obligation and the participant's existing permissions. An effectful
check must declare its retry/reconciliation contract. Reads of external state may
be nondeterministic; retain the observed evidence and result. Ledger replay never
reruns a tool, skill or script to reconstruct a verdict.

The ordinary peer flow is:

1. Author the claim and its validation contracts; resolve and freeze references.
2. Generate/post work and receive artifacts/testaments through their own lifecycles.
3. Read a coherent evaluation context for a ready check.
4. Record the applicable begin/phase fact, then invoke the referenced capability
   in the participant's own environment.
5. Commit a result artifact and fenced verdict; commit eligible dependent
   transitions atomically.
6. Issue any evidence-backed corrective or follow-up claim as an authorized peer.

If a participant needs another agent's help evaluating, it issues an ordinary
consultation/evaluation claim. That peer returns artifacts and a testament. The
original requirement's designated evaluator remains responsible for its accepted
verdict unless the immutable contract explicitly designates that peer. There is no
mandatory evaluation subclaim for a claimant evaluating its own quality bar.

## 5. Public operations to implement

The table below describes the independent-lifecycle target. Existing-profile
receipt, validation begin/result/complete, and standalone proof registration are
implemented through the narrow participant path documented in
[19](19-cli-mcp-implementation.md). They do not implement the new artifact or
testament state transitions and exact slot-bound evaluation contract. Mutations use the shared durable
request identity. Context/readiness reads use ordinary authenticated read requests
without reserving mutation ordinals. Both use the strict typed DTO path.

An initial observed context read is implemented as
`Client::validation_context(RequestEnvelope)`, CLI
`focal get validation ID --context`, and MCP `validation.context`. It composes at
most three existing reads at one exact read token: the pinned requirement and its
result-record page, its owning claim, and the claim's optional current closing
testament. It retains the results continuation, bounds the composed JSON by the
frame limit and the whole exchange by the lesser of the retry deadline and thirty
seconds. It does not fetch artifact payloads, infer a single-artifact target from
the manifest, grant begin/result authority, or lease execution. It observes the
existing model; the independent-state readiness and precise target contract below
requires the L2–L5 changes.

| Operation | Authorized intent and result |
|---|---|
| Artifact receipt/structural rejection | Claimant records observation or a typed receipt failure on a specific artifact |
| Testament posting/receipt | Testifier activates its frozen response; claimant acknowledges that exact response |
| Observed validation context — implemented read | Existing requirement, result page, owning claim and optional closing testament at one exact token; no inferred target or execution authority |
| Target-bound validation context/readiness — planned extension | Bounded coherent independent lifecycle state, exact target binding, run/phase, evidence references and current begin eligibility; a read grants no execution lease |
| Validation begin/phase | Designated participant records that a particular eligible check is beginning; no executor is launched |
| Validation result submission | Atomically records bounded result evidence and the designated evaluator's fenced result |
| Claim/testament evaluation advancement | Narrow participant-authorized request; owner derives/verifies permissible aggregate transitions |
| Child claim submission | Exact parent and receipt/standing proof included in hashed durable input; trusted cause is checked by the owner |
| Corrective/consult follow-up helper | Participant authors new work with exact evidence, immutable lineage and a bounded continuation budget |

Human commands must expose meaningful object verbs and IDs, with optional filters
for lists. A successful CLI mutation prints and flushes its result before managed
request retirement; MCP requires explicit result consumption. Transport request
acknowledgment is never artifact receipt, testament receipt, or validation success.

Protocol 3 now permits existing-profile peer operations after the owner checks
the committed role: the claimant records testament receipt and starts or completes
legacy aggregate evaluation; the designated evaluator submits its fenced verdict;
a participant registers its own proof artifact. These checks do not grant generic
Runtime authority or require global Evaluator registration. L5 must extend this
to independently eligible, exact-target check begins and atomic result evidence
under the new profile. Explicit committed delegation is required to exercise
another role, with any configured additional policy checked at the same boundary.
A combined report/verdict mutation remains the target for atomic result evidence.

## 6. Dependency-ordered implementation and acceptance

- [ ] **L1 — Freeze the four lifecycle contracts.** Define states, transition
  owners, single-artifact targeting, optional checks, result-evidence asymmetry,
  partial response cycles and aggregate precedence. Add executable transition
  tables and explicit tests for every forbidden writer. Keep external execution
  outside these transitions. [17](17-lifecycle-state-and-authority.md) now fixes the
  written contract and resolves source conflicts. The executable Rust contract
  now covers roles, independent evidence delivery, target-bound evaluation,
  exact-slot aggregation and audit closure; see [17 §11](17-lifecycle-state-and-authority.md#11-executable-contract-boundary).
  Owned non-artifact acceptance, complete Receipt proofs, graph failure witnesses,
  monitor/child registries and checked succession plans are now present. Complete
  effective-state integration, lineage validation on every creation path and
  owned-tree terminal/fencing consequences remain, so this gate stays open.
- [ ] **L2 — Upgrade stored formats safely.** Existing object bodies, WAL prepared
  intents, managed receipts and snapshots are frozen. Introduce versioned decoders
  and an actual durable floor transition, including all-voter activation and learner
  fences, before accepting new command or object encodings. Preserve old command
  hashes and exact historical outcomes. Do not infer missing historical lifecycle
  events or reuse the existing immutable managed-v1 fingerprint for a new format.
  In particular, historical `ArtifactAttached` deltas denote insertion into an
  evidence set, or standalone registration; they do not prove testament attachment.
  Old durable artifacts in open evidence sets must remain distinguishable from
  those actually referenced by a closed testament. Defaulting all old artifacts
  to Attached, as a Sylk migration might, is incorrect for Focal. See the concrete
  storage/replay/floor audit in [18](18-lifecycle-storage-upgrade.md).
  The full nested V1 checkpoint codecs, prepared domain-input codecs, shared
  frozen canonical command hash, strict complete-consumption checks and original
  workflow/codec fixtures are implemented. Explicit historical execution
  selection now owns the old reducer, validation and graph rules with fixed V1
  model semantics; current proposal policy remains separate. Surrounding Session
  rows, metadata/snapshot envelopes and receipt/control/membership/placement hashes
  now use frozen codecs with original-writer corpora and borrowed snapshot output.
  Consensus now implements one ordered durable decoder transition and confirms
  both compiled decoders before transitioned recovery. Its old-binary refusal and
  checkpoint retention are qualified. Production Session still uses only V1;
  actual successor definitions/decoders, broader historical branch fixtures,
  all-voter activation and learner fences remain open.
- [ ] **L3 — Implement artifact and testament state.** Add independent status
  history and transition authorization; stream artifacts first; make exact close
  attachment atomic; implement separate posting and claimant receipt. Test both
  possible receipt/close orderings, duplicate delivery and failure after restart.
- [ ] **L4 — Implement target-bound validation state.** Resolve explicit artifact
  slots/IDs; represent missing required targets without inventing an artifact;
  track independent check phases and result artifacts. Keep programmatic/quality
  ordering within a check and concurrency across checks. Test optional failure,
  short-circuit, late completion, stale receipts and unchanged terminal parents.
  Add bounded artifact/testament lifecycle indexes, exact validation-to-artifact
  edges and result-evidence relations. Existing claim-only lifecycle indexes and
  direct validation-to-claim aggregation cannot substitute for these projections.
  Publish the changed core rows and graph rows together under precharged capacity.
- [ ] **L5 — Expose peer evaluation.** Add coherent context reads, narrow begin,
  result and aggregate mutations, language-neutral schemas, Rust constructors and
  CLI/MCP parity. A normal Actor designated by the requirement must be able to
  complete the intended exchange without a global Evaluator registry, worker
  registration or unrestricted Runtime authority. Test the default two-Actor laptop
  flow and optional additional policy restrictions separately.
  An unrelated actor must not be able to start, impersonate, or complete it.
- [ ] **L6 — Bind external references and evidence.** Support code-first authoring
  and optional document forms for tool/skill/code contracts. Preserve exact
  definition versions and report provenance; no daemon tool invocation or script
  loader is introduced. Test an external programmatic client and an agentic
  participant using the same result protocol with no framework-specific server code.
- [ ] **L7 — Complete challenge and consult helpers.** Implement owner-checked
  child cause, proof requirements and participant-owned corrective/follow-up
  issuance. A missing or failing challenge proof must remain distinguishable from
  validator infrastructure error. Consult clarification normally creates a narrower
  consult; it does not automatically punish the respondent. Bound retries and
  follow-up depth without resetting budgets by rephrasing a question.
- [ ] **L8 — Qualify the complete peer exchange.** Exercise all four lifecycles
  with separate issuer/respondent identities, parallel checks, independent artifact
  receipt, generated-but-unposted testament, partial/failure testimony, result
  evidence, exact request replay, restart and quorum failover. Assert that no agent
  or worker is launched by Focal and that only committed, authorized evidence
  changes aggregate state.

Required regression cases include: artifact A observed before artifact B exists;
close before any per-artifact receipt; two validations finishing in the opposite
order; quality pending while programmatic siblings finish; an optional failure
after required work succeeds; missing required artifact at close; a validator
execution error with intact respondent proof; and an evaluator result arriving
after a receipt adoption or parent cancellation. Tests must inspect each object's
own recorded history, not only the final claim status.

The managed CLI/MCP delivery and receipt-shape fixes qualified in
[09](09-implementation-status.md) remain useful prerequisites. Their passing tests
do not establish completion of L1–L8 or the original P00–P20 goal.
