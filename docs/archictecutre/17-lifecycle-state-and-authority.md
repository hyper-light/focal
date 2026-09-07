# Four-family lifecycle state and authority contract

Status: **target contract with an executable Rust contract layer, 2026-09-06**.
This freezes L1's domain decisions for [16](16-peer-validation-contract.md).
The [lifecycle module](../../crates/focal-model/src/lifecycle/mod.rs) implements
the role, state, target, aggregation and audit rules described in §11 below;
L1's remaining owner-integration obligations keep that gate open. The first
`Core<NativeState>` owner retains claims, complete validation definitions,
independent Admission/Increment/Receipt evaluations, evidence, responses and request
outcomes in custom RAM storage. Posting, evaluator entry and result publication are
checked owner transactions; cancellation and supersession atomically fence
registered evaluations. These rules are
not installed in the running ledger. [18](18-lifecycle-storage-upgrade.md) remains
the prerequisite for storing or applying successor semantics. Existing records
retain their original interpretation.

The tables below define permitted transitions, not serialized discriminants or
currently available commands. An omitted transition is forbidden. The issuer,
respondent and designated evaluator are participants; none denotes a Focal-owned
agent, job, worker pool or process. A single participant can invoke its own tool
or skill and submit a result without issuing an evaluator subclaim.

## 1. Authority, sources and vocabulary

The user's peer-to-peer boundary has precedence. Hecate's
[LEDGER §§2–4](reference/hecate/docs/architecture/LEDGER.md) supplies immutable
content, generated/post distinction, proof-versus-delivery, and graph semantics;
[LEDGER_CORE §2](reference/hecate/docs/specs/LEDGER_CORE.md) requires checks against
effective state and publication only after commit. Sylk's
[ARTIFACTS_AND_VALIDATIONS](../../../sylk/docs/ARTIFACTS_AND_VALIDATIONS.md) §§2,
5, 7, 9 and 11 supplies the detailed four-family coordination. Its §2 explicitly
takes precedence over conflicting older lifecycle wording. Sylk's
[CLAIMS_AND_TESTAMENTS_LIFECYCLE](../../../sylk/docs/CLAIMS_AND_TESTAMENTS_LIFECYCLE.md)
§§3.3, 4.X, 5–6 supplies response activation and claim propagation. Section 9 below
records the Focal resolutions where those sources conflict with each other or
with existing Focal history.

| Term | Exact meaning in this contract |
|---|---|
| Claimant / issuer | Participant who authored the claim and its immutable acceptance requirements; these names identify the same role |
| Subject / respondent / testifier | Subject receives the directed claim; its current authorized receipt holder acts as respondent, and as testifier when closing a response |
| Execution receipt | Durable holder and generation entitlement to perform/respond to a claim; distinct from every observation or acknowledgment below |
| Artifact receipt | Claimant's observation of one unattached work artifact, or its typed structural rejection; does not imply evaluation |
| Testament receipt | Claimant's observation of one posted response; the fact used by pure delivery validation |
| Evaluator | Authenticated Actor principal designated by the committed requirement/phase, not a mandatory global security role; the designation does not implicitly confer receipt, response-authoring or claim-cancellation authority |
| Work artifact | Immutable respondent evidence with a declared slot/name and its own lifecycle |
| Artifact slot | Bounded immutable name in a requirement; resolves to exactly one ID and digest within an individual response, never by schema similarity |
| Response testament | Respondent-authored immutable summary, confidence, reported outcome, work-slot manifest and diagnostic evidence for one finished or failed work cycle; distinct from the claim's aggregate state |
| Result artifact | Typed evidence of an evaluation attempt or phase, authored by its authorized evaluator; its lifecycle ends at Generated |
| Result testament | Claimant-issued immutable audit bundle of result artifacts for the original claim; its lifecycle ends at Posted and never drives claim satisfaction |
| Validation | Immutable declared check plus independently recorded evaluation state, attempts and evidence, scoped to an exact target |
| Required / Observe | Required outcomes constrain acceptance; Observe outcomes remain evidence and never delay or fail required completion |
| Terminal cut | Committed position and cause that made an object terminal; it is a lifecycle fact, not wall-clock time or a mutable severity score |

An evaluation state is addressed by validation ID, declared phase, exact target and
evaluation generation. Multiple response artifacts may create separate evaluation
states for the same immutable requirement; a latest-run pointer cannot erase them.
Attempts and phases belong to that validation's lifecycle records, not a fifth
business object family. Retrying a tool after an allowed execution error is an
attempt transition; it never resets a terminal validation to Ready.

Avoid the unqualified verb “acknowledge” in public contracts: distinguish request
result consumption, stream cursor advancement, artifact receipt and testament
receipt. Avoid “dispatch” as a Focal action: recording BeginEvaluation does not
invoke a participant's tool. “Ready” means a recorded validation state; it is not
permission to start if target, parent or authority fences prohibit the transition.

## 2. Invariants applying to every table

1. Content identity, payload bytes, requirement mode, target declaration, evaluator
   designation and definition version freeze at generation. Attachment and history
   live in lifecycle/typed relation records; no transition edits authored content.
2. A mutation binds authenticated principal, ledger, object IDs, content hashes,
   expected revisions and every applicable receipt/evaluation fence. The owner
   derives authority from installed state, not caller-authored lifecycle fields.
   Being an enrolled Node or reading a context grants no domain authority.
3. An authenticated Actor matching the committed evaluator designation may begin
   and report its check, without a separate global Evaluator registry. The issuer may record
   receipt and request deterministic advancement; it cannot impersonate a different
   evaluator's result. Only when delegating receipt or response authority beyond
   those default domain roles is an explicit committed delegation required; a generic
   evaluator reference is insufficient. Additional evaluator capability/attestation
   checks apply only when required by the actual policy, not automatically to every
   custom participant.
4. The issuer's immutable requirement authorizes Focal to derive that check's
   aggregate consequences when its designated evaluator reports. This is verified
   reduction of participant-authored facts, not autonomous judgment. No public
   operation accepts an arbitrary requested parent status.
5. Checks run on effective state including earlier admitted mutations. A generation,
   begin or result becomes externally actionable only after durable commit. Exact
   retry returns the same outcome; changed input under the same request identity
   conflicts. Request retirement never changes any of these object states.
6. A terminal object's content, status and original terminal cause do not change.
   Later evidence may be related to it without reentering its state machine. An
   explicit successor claim or new target evaluation has a new identity.
7. Deadline inputs and authority changes are committed facts. Replay does not read
   the clock, invoke code or infer that a disconnected participant failed a check.

8. Acquiring a receipt creates no testament. The current respondent must author a
   response when its work completes or fails. Every non-Complete report includes
   real error/failure artifacts; partial work may accompany them. The ledger does
   not fabricate an absent agent's account or turn a timeout into its testimony.
9. Reported outcome and evaluated acceptance are different facts. Summary,
   confidence and reported outcome freeze with the response. Error evidence is
   inspectable even when requested work could not be produced; it never becomes a
   successful work-slot witness merely by being attached.

## 3. Claim transitions

Retain Focal's existing twenty claim status names and graph predicates. The new
profile changes how independent evidence produces those facts; it does not turn
artifact or testament status into aliases for claim status. “Open” means any
nonterminal claim, but Generated is not actionable.

| Prior state | Authorized fact | Next state / consequence |
|---|---|---|
| Absent | Issuer generates valid immutable claim and requirements | Generated; no work receipt or execution eligibility |
| Generated | Issuer posts under current standing/target rules | Posted; applicable admission checks become eligible |
| Generated or Posted | Authorized final post/admission failure with typed evidence | PostFailed |
| Posted | Subject acquires receipt after admission and graph start predicates pass | Received; record exact holder and generation |
| Posted | Authorized final receipt-boundary failure | ReceiptFailed; a transport timeout alone is not this fact |
| Received or Progressed | Current receipt holder records progress | Progressed; no local-completion or graph-release effect |
| Received or Progressed | Respondent generates its first response and freezes its manifest | TestamentGenerated, atomically with response Generated and manifest attachment; response remains unposted |
| TestamentGenerated | Respondent posts an eligible response | Claim phase unchanged; the independently Posted response is now eligible for claimant receipt |
| TestamentGenerated | Claimant first receives an eligible posted response under the current receipt, regardless of authored cycle order | TestamentAcknowledged; record pure delivery validation without acknowledging any other response |
| TestamentAcknowledged | Claimant requests aggregate entry or the committed designated evaluator begins its eligible whole-work check | Validating; affected response/artifact/check states advance together under their exact role guards |
| TestamentGenerated, TestamentAcknowledged or Validating, before local completion | Eligible later response is generated or posted, or received after the first actual claimant receipt | Claim's attained phase unchanged; record the separate response's transitions and exact lineage |
| Validating | All required acceptance witnesses exist | Record local completion; Satisfied only when declared graph predicates also hold |
| Validating | An uncovered required cause becomes terminal under §7 | ValidationIncomplete, ValidationFailed or ValidationErrored |
| Received or Progressed | Current holder records an inability to assemble or submit its response, with a real diagnostic | Retain a nonterminal closing incident; preserve the opportunity to submit an ordinary respondent-authored failure response |
| Open | Authorized explicit cancel, revocation or supersession | Cancelled, Revoked or Superseded; fence further evaluation authority |
| Open | Matching committed deadline, dependency failure or deadlock witness | Expired, DependencyFailed or Deadlocked respectively |
| Terminal | Exact retry, observation, allowed late evidence or new successor | Existing terminal status/cause unchanged |

The executable successor contract restricts final post failure to the issuer and
receipt-boundary failure to the subject. A closing incident belongs to the current
receipt holder and retains its diagnostic without terminalizing the claim. Failed
work follows ordinary response generation, posting, claimant receipt and checking.
A closing incident is not a response and cannot acknowledge delivery or satisfy a
requirement. Publication must atomically retain its immutable incident and revised
claim, with no intermediate row visible.

The historical V1 `FailTestamentGeneration` reducer remains for replay only. New
admission rejects that runtime-generated, automatically acknowledged testimony;
new callers use respondent-authored `CloseTestament`. Existing stored terminal
`TestamentGenerationFailed` history and exact retained retries stay unchanged.
An unavailable server that cannot commit has no durable failure fact to report.
Progress on terminal work remains observational/no-op according to the existing
command contract. Receipt adoption changes entitlement/generation, not content or
the claim's attained phase, and fences stale response/evaluation mutations.

Later response generation, posting and receipt do not regress the claim's attained
phase. Their own histories record those transitions. Local completion is not
satisfaction: `DependsOn`, `Awaits`, SCC/fixpoint propagation, release and monitor
witnesses retain the rules in [02](02-domain-and-lifecycle.md) §4. A cycle cannot
become satisfied merely because its members have posted responses.

## 4. Artifact transitions

Each work artifact has exactly one response attachment. A reference reused as
input evidence does not reattach or reopen that artifact. A different response may
produce a new artifact referencing prior evidence, with its own ID and lifecycle.

| Prior state | Writer and guard | Next state / atomic effects |
|---|---|---|
| Absent | Current respondent commits valid typed immutable work evidence with durable custody | Generated; visible before response generation |
| Absent | Respondent can durably identify an unsuccessful production and typed diagnostic | GenerationFailed; no invented payload, terminal |
| Generated | Claimant observes the unattached artifact | Received; acknowledgment only |
| Generated or Received | Claimant supplies a supported structural/metadata rejection before attachment | ReceiptFailed with typed cause, terminal |
| Generated or Received | Respondent closes the exact current evidence set | Attached, in the same commit as response Generated |
| Attached | Eligible claimant/evaluator begins evaluation after response Posted and Received | Validating, atomically with the first eligible check begin |
| Validating | Every Required check for this exact artifact passed | Validated, atomically with the last triggering result |
| Validating | First Required blocking result for this artifact commits | ValidationFailed with Incomplete/Fail/Error/quality cause, atomically with result |
| GenerationFailed, ReceiptFailed, ValidationFailed or Validated | Any attempted status progression | Reject; no reset, late evidence cannot rewrite status |

Generated → Attached is a valid path when closing wins the race with observation.
Do not manufacture Received. If observation commits first, record Received →
Attached. A later structural receipt request cannot insert ReceiptFailed after
Attached; malformed input detected by an actual check is instead its typed
evaluation outcome. Claimant observation of a still-unattached artifact can occur
after the claim becomes terminal; it preserves the artifact's original indexed
receipt/cycle and does not change the claim. Schema-invalid new artifact envelopes
are refused at admission, not accepted solely to construct a lifecycle failure.

GenerationFailed and ReceiptFailed artifacts cannot also transition to Attached.
The response's attachable manifest excludes them. Its separate `failed_work`
references freeze each exact failed binding, slot, state and diagnostic from the
owner's complete work set. A production failure uses the actual respondent's
Production diagnostic as its binding; a receipt failure retains the real rejected
output and the claimant's structure/metadata diagnostic. Neither invents a product
or supplies a successful slot binding. Missing required evidence is assessed when evaluation
begins, not on the first streamed artifact, so producing artifact A before B does
not prematurely fail the claim. No missing artifact object or placeholder hash is
invented. Failure to create any durable artifact is recorded against the nearest
existing claim/response when possible.

An artifact with zero Required artifact checks can validate on authorized
evaluation entry; this truthfully means no required check blocks it, not that its
quality was established. Its Observe checks remain separately visible. The claim
still has the mandatory Required WholeWork delivery check. Result artifacts use
§8 instead of this table. Custody verification/revision is orthogonal to all states.

## 5. Response testament transitions and multiple work cycles

| Prior state | Writer and guard | Next state / atomic effects |
|---|---|---|
| Absent | Current respondent authors a bounded summary, confidence and reported outcome with exact work bindings and durable diagnostic references after its work ends | Generated + every attachable manifest artifact Attached; freeze report, bindings, failed-work references and diagnostics and advance the first response's claim to TestamentGenerated |
| Generated | Respondent activates the exact frozen response under the same valid entitlement | Posted; claim retains its attained phase, with a posted-response fact |
| Posted | Claimant acknowledges that exact response under its matching receipt fence | Received; first response advances an open claim to TestamentAcknowledged; a terminal or locally complete claim remains unchanged; an eligible pre-deadline pure Receipt passes; see the deadline and late-observation rules below |
| Received | Authorized evaluation begin | Validating + eligible artifact/check entry facts |
| Validating | All its required slot groups have exact successful witnesses and delivery passed | Validated; derive claim aggregate under §7 |
| Validating | Required slot absent or supported structural failure prevents supplying it | ValidationIncomplete; derive claim aggregate under §7 |
| Validating | Required present artifact fails its standard, including quality failure | ValidationFailed; derive claim aggregate under §7 |
| Validating | Required evaluator/tool/reader cannot establish a result after permitted retries | ValidationErrored; derive claim aggregate under §7 |
| Any terminal response state | Later result, correction or retry | Original state/cause remains; evidence and successors use explicit relations |

Native claimant receipt materializes each declared pure Receipt evaluation only
for an open claim, using the actual received report stamp, receipt and cycle.
Before its deadline it records an artifact-free Pass. At or after the deadline,
the response still becomes Received but the evaluation remains Ready; no Pass or
terminal failure is invented. Receipt after claim terminalization or local
completion is observational only and does not create another evaluation.

The same open-claim receipt registers every declared WholeWork slot check as
Ready, including Observe checks. Its target pins the actual attached artifact's
binding or an explicit MissingSlot when the frozen manifest lacks that slot.
This does not begin a check or decide acceptance. Required Increment outcomes
and target sealing gate subsequent WholeWork entry; missing evidence is assessed
there, rather than treating receipt as an implicit validation failure.

Authored cycle order does not impose delivery order. The first actual claimant
receipt under the current entitlement advances a pending claim to
TestamentAcknowledged, even if an earlier response is still Posted. Receiving
that earlier response later preserves the attained claim phase. Each response's
own received fact and exact evidence remain independent; receipt adoption still
rejects delivery under an abandoned entitlement.

Complete, Partial, Refused, Impossible, Interrupted and Failed are explicit
respondent-authored outcomes. None defaults to Complete. Every non-Complete
outcome requires at least one typed durable error artifact belonging to the
current respondent, claim, receipt and work cycle. A response with diagnostics
and no work-slot bindings is valid when no requested output could be produced.
The claimant can receive and inspect it; MissingSlot is assessed at evaluation.

Native responses retain respondent diagnostics, failed-work references and
attachable work-slot bindings separately. A diagnostic must not masquerade as a
requested output. The exact Production diagnostic may occur in both a failed-work
reference and the respondent's diagnostic list. A claimant rejection does not
satisfy the respondent diagnostic requirement for a non-Complete report. Complete
may coexist with failed-work references: that assertion cannot erase the failure
or establish acceptance. Summary bytes and all three reference collections are
bounded and copied once after construction preflight. Private semantic identity guards bind both
report and evidence to prepared transitions and claim observations; a reused
external binding cannot substitute a changed report. Storage must canonically
encode the same authored fields under the successor format in [18](18-lifecycle-storage-upgrade.md).

Partial, Refused, Impossible, Interrupted and Failed reports use the ordinary
response lifecycle, including explicit claimant receipt and independent checks. A request whose
post did not commit leaves a real Generated response available for exact retry;
it does not activate it or delete it. The source's generic “failed posting” wording
does not add an undeclared response status. A durable refusal of activation is a
request outcome/diagnostic; claim expiry/cancellation retains its own authority.

Focal resolution of the D-03/Sylk difference: the **new** profile permits bounded
multiple response cycles with explicit claim, receipt generation and prior-response
lineage. A cycle has one immutable response and unique per-response slot names.
The default human path still produces one response with no cycle configuration.
Additional cycles require explicit identities only when used, cannot overwrite the
previous response or merge its manifest, and cannot begin after claim terminality
or local completion. Receipt adoption does not automatically adopt old responses
as fresh evidence; any eligible continuation must explicitly validate its lineage.

If adoption occurs before the original response is received, the first eligible
delivery under the replacement entitlement may advance TestamentGenerated to
TestamentAcknowledged. This preserves progress without accepting the old receipt:
the replacement response still has its own explicit cycle, prior-response lineage,
posting and claimant receipt. It does not rewrite the original response's history.

Multiple responses can be posted before the claimant chooses to evaluate them.
There is no promise that a later response can repair a claim already failed by
evaluation. Additional responses participate only under §7 at the exact committed
prefix. The single active testament pointer in V1 Core remains the old profile;
supporting this target requires new indexed lifecycle data and versioned commands.

## 6. Validation target, phases and outcome transitions

Work-evidence checks bind one declared slot to one artifact ID and content digest
in one response. All Required checks for a slot must pass on the **same artifact**;
passing one check on artifact A and a different check on artifact B is not a pass
for that slot. Different checks have no hidden dependency on each other's outputs.
The optional quality phase may consume its own deterministic result and target.
Requirements for relationships between artifacts must explicitly identify the
evidence being evaluated; do not broaden a one-artifact check into an implicit
multi-artifact scan.

A declared agentic-only check enters its agentic phase directly; it does not
invent a successful programmatic result. A declared programmatic-plus-quality check
requires the actual programmatic Pass first. Focal always verifies structural
admission, role, schema/custody and target/fence conditions in either case; those
checks are not a substitute for an optional participant-executed programmatic
validation. The existing scheduler has a direct-quality branch, but current
client and Core admission reject a quality bar whose first handler is agentic.
That branch therefore does not make agentic-only quality requirements available
to new callers. The successor profile must admit and qualify this path while
preserving the old decoder and replay rules.

Pure Receipt is the explicit non-artifact exception: Required WholeWork delivery
of the exact posted/received response, with no handlers, schemas or quality bar.
It passes in Core on authorized testament receipt. Test, Inspection, Contract and
the other evidence-check kinds must not use Receipt as an evidence bypass.
Admission and Increment retain distinct declared readiness/target rules; neither
silently becomes whole-work evaluation of an unattached artifact. Their target
variant is explicit and their records do not fill an unrelated WholeWork slot.
A registered Increment can begin against its real ReceiptFailed output under live
authority: claimant rejection prevents attachment and WholeWork progression, not
that independent check. The evaluator must report the actual outcome; Focal does
not synthesize Incomplete. No Increment result rehabilitates the rejected artifact
or fills its response slot. GenerationFailed has no produced output and cannot
materialize or begin an Increment.

This keeps Sylk's terminal artifact states (§5.1) separate from independent
validation state. Focal applies the Attached → Validating dispatch and
short-circuit rules in
[Sylk §§7.7 and 11.1](../../../sylk/docs/ARTIFACTS_AND_VALIDATIONS.md) to WholeWork
validation. Extending them to forbid every Increment on a rejected output would
leave a registered Required Increment without an attainable outcome, contrary to
the phase boundary in [02 §6.3](02-domain-and-lifecycle.md). The source artifact
still remains terminal; only the separate evaluation progresses.

The semantic state set is Ready, Validating, ValidatingQualityBar, Validated,
ValidationIncomplete, ValidationFailed, ValidationFailedNotRequired, Errored,
ErroredNotRequired, QualityBarValidationFailed and
QualityBarValidationFailedNotRequired. This is eleven states; Sylk's introduction
says ten while its actual list contains eleven. The mode remains explicit in every
record, including terminal evidence. Capitalization here does not allocate new
wire numbers.

| Prior state | Authorized fact and guard | Next state / result |
|---|---|---|
| Absent | Issuer declares a requirement and eligible evaluation target context is materialized | Ready; immutable declaration retained independently of this evaluation |
| Ready | Exact designated evaluator begins an eligible declared programmatic phase under live fences | Validating; pin target, definition, attempt, deadline and evaluator |
| Ready | Exact designated Actor evaluator begins a declared agentic-only check and satisfies any explicit additional policy | ValidatingQualityBar with pinned target/definition/attempt; no synthetic programmatic Pass |
| Ready | Required target is absent from a closed, posted and received response at evaluation begin | ValidationIncomplete; name missing slot, no invented artifact or tool invocation |
| Ready | Observe target absent or parent/artifact short-circuits before begin | Remains Ready with a recorded unavailable/suppressed reason; no fabricated result |
| Validating | Programmatic Pass with no quality phase | Validated with result evidence |
| Validating | Programmatic Pass and declared supported quality phase | ValidatingQualityBar, retaining deterministic evidence |
| Validating | Conclusive programmatic Fail | ValidationFailed or ValidationFailedNotRequired by mode; no fallback on failing proof |
| Validating | Evaluation Error; ordered retry/fallback remains within pinned policy | Same lifecycle state, next recorded attempt; no terminal parent propagation yet |
| Validating | Final Error after permitted attempts | Errored or ErroredNotRequired by mode |
| Validating | Required input is proven incomplete | ValidationIncomplete; preserve exact diagnostic evidence |
| ValidatingQualityBar | Designated Actor evaluator reports Pass under the declared quality contract and any explicit additional policy | Validated with quality evidence |
| ValidatingQualityBar | Designated evaluator reports conclusive Fail | QualityBarValidationFailed or QualityBarValidationFailedNotRequired |
| ValidatingQualityBar | Evaluation cannot complete after its permitted attempts | Errored or ErroredNotRequired; not a quality Fail |
| Terminal evaluation | Another result or new begin | Exact retry returns prior outcome; conflicting replacement rejected |

Missing-slot settlement is a structural claimant operation. It checks the exact
live claim, received report, frozen manifest, declared slot, receipt, generation
and Required Increment gate. It invokes no handler, so a handler deadline or
handler policy grant cannot delay that assessment or let an Observe omission
block response entry. Required absence records an artifact-free Incomplete;
Observe absence records suppression. An already explicitly fenced or sealed
evaluation retains its original state without a new result. Required slot
presence still has its independent consequence at response entry. Generic
external Begin and Report retain their deadline, policy and authority checks.

An Observe evaluation that establishes Incomplete retains that outcome as
nonblocking, including its explicit mode. The blocking test is mode plus outcome,
not a suffix guessed from a status name. If encoding uses ValidationIncomplete
for that case it must preserve mode; an additional numeric variant is an L2 schema
decision, not permission to discard missing optional evidence.

Focal D-06 governs the declared quality evaluation contract: issuer category is
irrelevant, and the ordinary permission is the authenticated Actor's exact match
to the committed evaluator designation. A global Evaluator registry, worker
registration or Runtime grant is not required for a custom laptop participant.
Programmatic/agentic execution kind describes the required evaluation; Focal cannot
attest arbitrary external execution from identity or an `agentic: true` field.
If applicable policy expressly requires additional evaluator approval, capability
or execution evidence, enforce its committed facts at the same boundary. Such a
policy adds requirements only where requested; the default does not invent them.
A tool timeout or unavailable dependency
is Error unless the immutable test contract explicitly defines the observed timeout
as the tested work's conclusive failure. Admission/capacity refusal before begin is
neither a run nor a business Fail.

## 7. Aggregation, short-circuit and races

The **new profile** uses the following deterministic reduction at each ordered
mutation. This replaces wait-for-all/severity selection only for newly activated
lifecycle semantics; it must not change legacy replay.

1. Validate all direct facts and derive each exact artifact's triggered state.
   A Required blocking result terminalizes that artifact immediately. An Observe
   result never blocks. Already terminal artifact history is not repainted.
2. Derive the affected response's own outcome from its manifest and Required slot
   groups. Its failure remains true even if another response satisfies the claim.
3. Before terminalizing the claim, find eligible successful slot witnesses among
   its already Posted, Received and evaluated responses, including facts committed
   in this mutation. Each witness includes response ID, artifact ID/digest and all
   Required check result IDs for that artifact. Schemas are never selectors. An
   alternative response must independently pass delivery; Generated-only evidence
   cannot supply an alternative witness.
4. If all Required slots and non-artifact obligations are covered, record local
   completion with exact witnesses, then apply graph predicates. A failed response
   must not discard an already-valid alternative successful artifact. Optional
   evidence is not added to the acceptance set merely because it exists.
5. Otherwise the first newly committed blocking cause for an **uncovered Required
   slot/obligation** selects the claim's terminal outcome. Missing/structural loss
   maps to Incomplete; conclusive programmatic/quality failure to Failed;
   evaluation infrastructure failure to Errored. Merely pending work has no terminal
   verdict. A response missing a slot can therefore fail a claim when evaluation
   begins if no eligible alternative already covers it.

“First” means committed mutation order and the defined transition order within that
mutation. Within one mutation, independent causes sort by response ID bytes, then
the requirement's index in the immutable claim declaration, then evaluation
generation, attempt index and phase order. Programmatic precedes quality; a missing
target uses its declaration index without inventing a run or artifact. Reject two
conflicting causes for the same complete key. This order never means worker
completion wall time, map iteration, or a later stronger severity.
All simultaneous causes remain available as evidence. Exact retries do not append
a second cut. A previously committed failure cannot be healed by a response that
arrives afterward; the participant issues a successor claim when new work is needed.

For mutations mixing target families, the successor contract orders Admission,
then Increment by exact artifact ID and digest, then Response by response ID.
Within a target it uses declaration index, generation, attempt and phase as above.
This typed key also orders audit members, so two increments of the same declaration
cannot collapse into one claim-level target. It does not change V1's historical
severity ordering.

The immutable acceptance manifest includes every declared check and slot-presence
index. All declared Required pure Receipt checks must pass; receiving only one
does not bypass another. Admission gates receipt acquisition. Increment targets
are registered before exposure and their set is explicitly sealed before whole-work
entry. A response can close, post and be received while increments finish. Required
increments need final outcomes before whole-work checks begin; a failing increment
does not immediately terminalize the working claim, but constrains its eventual
acceptance. Observe increments do not delay that entry. Final audit sealing closes
all target registration separately; it never prevents already-begun evaluations
from finishing under their live fences. `SealIncrementTargets` closes only new
Increment target membership; already-registered Ready checks may still begin and
begun checks may report. Respondent diagnostics and failure testimony remain
possible. Starting a check on the last already-closed response cycle does not
consume another response allowance.

On a blocking WholeWork artifact validation failure, Ready sibling checks on that
artifact become ineligible to begin; they remain Ready with a suppression cause. If the claim also
terminalizes, other unbegun checks on that claim become ineligible. A failed artifact
whose slot is covered by a valid alternative must not suppress unrelated work still
needed by the open claim. No Skipped verdict or falsely terminal Ready status is
invented. Context reads return state **and** start eligibility/reason.

Already-begun evaluations may finish their pinned attempt/phase chain, including a
declared quality phase, for audit under their original bounded policy. They cannot
create a new evaluation generation, change target, or reopen terminal parents.
Every accepted late result still passes receipt, evaluator and policy fences.
Cancellation, revocation, supersession, expiry, receipt adoption or an explicit
evaluation fence invalidates completion authority as declared; late business
results then reject even if an external program actually completed. The participant
may retain its local output; Focal must not accept it as the active verdict.

Results and aggregate transitions become visible at one committed prefix. There is
no interval in which the API exposes the triggering final verdict but a still
advanceable parent that should have terminalized. The same rule covers graph release
and affected monitors; a bounded internal continuation may delay publication, not
expose a partial business transaction.

## 8. Asymmetric result evidence and audit closure

| Object/role | Permitted path | Authority and exclusions |
|---|---|---|
| Result artifact | Absent → Generated, terminal for this role | Actual designated evaluator commits typed attempt/phase evidence; producer remains that evaluator |
| Result testament | Absent → Generated → Posted, terminal for this role | Claimant freezes and posts the claim's bounded audit bundle; no respondent receipt is impersonated |

The role is explicit immutable content/typed relation data, not inferred from
`kind` text, coincident issuer/subject identity, or the current claim status. A
result artifact relates to original claim, requirement, exact target and run/attempt;
its result-testament membership is a relation, **not** Artifact Attached. The result
testament points to the original claim and cannot replace its response collection.
Neither kind of audit object enters ordinary response acceptance or starts a
recursive validation obligation. Another peer's ordinary evaluation claim still
has an ordinary response lifecycle of its own.

Accepted result evidence and its verdict are committed together, referencing already
durable bytes. Result artifacts may therefore become visible before the audit
bundle closes. Pass/Fail requires the declared proof; Incomplete/Error can instead
carry a typed diagnostic explaining unavailable evidence. A self-report is recorded
with its real principal and validator version, not described as independent proof
that arbitrary external code ran.

Sylk §9's “all required validations terminal” cannot literally close the audit
bundle after §7's short-circuit leaves unbegun siblings Ready. Focal resolution:
record the evaluation cohort when the local outcome is sealed (local completion or
terminal failure/control), before any remaining graph wait. No previously unbegun
check may enter afterward. Unbegun suppressed checks contribute
their suppression reason, not a result. Wait for begun checks, including begun
Observe checks, to finish or acquire an explicit terminal authority/deadline fence.
Only then may the claimant freeze the immutable per-claim result bundle in canonical
evaluation/attempt/phase order. A check allowed to start afterward would violate that
sealed cohort and is rejected. Audit closure does not delay an already justified
business terminal transition. Lost external results do not justify invented Pass,
Fail or cancellation; use the actual logged diagnostic/fence and preserve missing
result provenance.

No unbounded wait may retain uncharged data. Declared evaluation deadlines,
bounded result bytes/attempts and reserved completion admission cover this interval.
If required archive/retention is unavailable, pause new admission before capacity
is exhausted; do not drop late authorized results silently. Corrective or consult
follow-up work remains an explicit participant decision with immutable cause and
proof. Neither a failed check nor closing an audit bundle launches or authors it.

## 9. Source conflicts and historical migration

| Source tension | Frozen Focal resolution |
|---|---|
| Sylk board “does not compute aggregates” versus Focal pure reducer/fixpoint | Participants supply authorized facts; Focal derives/verifies the required atomic consequences without executing validators |
| Sylk ten-state prose versus eleven listed validation states | Use the eleven semantic names; retain explicit mode, including optional Incomplete |
| Sylk Required-only acceptance versus isolated “any artifact” failure wording | Only declared Required slots/checks block; optional failures remain visible |
| Claim TestamentGenerated on durable generation versus testament posting prose | First response generation advances claim TestamentGenerated atomically with attachment; posting activates the response without delaying or inventing that claim fact |
| Terminal receipt failure versus attachment of every referenced artifact | Failed artifacts remain terminal and are diagnostic references, not successful attached manifest entries |
| First failure versus wait-for-all severity | New profile uses committed causal cut and already-valid alternatives; legacy Focal uses its original all-required-final and Fail > Error > Incomplete > Pass |
| Multiple posted responses versus D-03 | New profile uses bounded explicit response cycles and exact acceptance witnesses; existing single-close records do not gain this behavior |
| All-required-terminal result bundle versus unbegun suppressed Ready siblings | Close only after begun cohort finishes/is fenced; record suppressed siblings without fake results |
| Non-agent issuer restriction | Retain D-06's declared agentic contract and committed principal designation; no issuer category ban or mandatory global Evaluator registry; enforce extra capability restrictions only when policy requires them |
| Programmatic-plus-quality path versus agentic-only requirements | Agentic-only enters its declared phase directly; only a declared programmatic phase requires its actual Pass before quality; structural admission is always enforced |
| Hecate no compatibility/Sylk no versioning versus existing Focal storage | Immutable data is not wire-format immutability; preserve original decoders/reducer semantics and activate new formats explicitly |
| Sylk legacy artifact default Attached | Never use that default for Focal's open evidence sets or standalone registered artifacts |

Migration must retain these distinctions as data, not reconstruct convenient status
labels after restart:

- A historical artifact's creation and custody revision do not prove claimant
  receipt, response attachment or validation. Historical `ArtifactAttached` deltas
  mean evidence-set insertion/registration; only an actual closed manifest proves
  manifest membership. Open-set and standalone artifacts remain distinguishable.
- Historical response creation and acknowledgment do not prove separate Posted,
  Validating or terminal response transitions. Show the known legacy facts/profile;
  do not populate invented state-history entries.
- Existing validation runs, attempts and verdicts retain exact target/fence meaning.
  Manifest/schema binding does not become named single-artifact binding by guessing.
- Historical open artifact kinds and valid legacy content remain readable and
  replayable. Stricter new declaration/schema rules cannot reject unrelated old WAL.
- Historical malformed strong Receipt outcomes already committed remain exact.
  New Generate/Batch/Supersede admission and new acknowledgment/whole-work requests
  reject that malformed requirement; cancellation and valid successor claims remain
  possible. The dedicated old-replay regression preserves past Pass rather than
  retroactively judging it under the new rule.
- An open legacy claim cannot silently switch aggregation/response profiles on
  upgrade. Finish under its original semantics where still permitted, or use an
  explicit authorized successor/migration operation with new immutable bindings.

The current evidence is in [objects.rs](../../crates/focal-model/src/objects.rs),
[validation.rs](../../crates/focal-core/src/validation.rs), and the original
[vocabulary](../../crates/focal-model/src/vocabulary.rs). Existing Core tests
`complete_receipt_workflow_replays_exact_state_and_deltas`,
`managed_complete_workflow_matches_legacy_domain_state_and_replays_exactly`, and
`historical_strong_receipt_replay_is_exact_but_new_acknowledgment_is_fenced` establish
legacy behavior, not completion of these target tables. See
[tests.rs](../../crates/focal-core/src/tests.rs) and
[managed_tests.rs](../../crates/focal-core/src/managed_tests.rs).

## 10. Atomicity, resource and read acceptance gates

Each item below must become an executable fixture through the same shared
mutation/read path used by native Rust, human CLI and MCP:

| Fixture | Required assertion at every published prefix |
|---|---|
| Artifact A arrives before B; close wins/loses observation race | A has independent visible history; only actual observations become Received; Generated strictly precedes response generation |
| Close, post and receipt separately; authorized compressed flow | Manifest freeze/attachment atomic; unposted response cannot trigger receipt/evaluation; compressed flow retains all real ordered facts and all writers' authority |
| Unrelated Actor, respondent posing as claimant, issuer posing as designated evaluator | Each unauthorized receipt/begin/result/aggregate rejected; Node enrollment and context reads grant no substitute capability |
| Two ordinary authenticated Actors with a custom external tool/skill | Claimant receipt and designated evaluator begin/result succeed under committed claim roles without global Evaluator registration or Runtime; explicit extra policy restrictions are tested separately |
| Two same-schema artifacts, duplicate/missing slot, two failing artifacts with complementary passing checks | Exact ID/digest bindings; no ambiguous selection or cross-artifact manufactured pass |
| Required fail with a begun sibling and an unbegun sibling | First cut and all triggered parents atomic; begun late result audit-only; unbegun stays Ready/ineligible |
| Already-passing alternative response, then failing response | Preserve both response states; successful Required slot witness checked before any claim failure |
| Failing response, then later passing response | Terminal claim unchanged; no reopening via delayed delivery |
| Observe failure/missing evidence after required success | Result visible; parent truth unchanged; no hidden successful check |
| Quality phase and error-only fallback | Exact designated evaluator, declared execution kind, any expressly required policy evidence, pinned chain and typed Error/Fail distinction; no daemon invocation |
| Result bundle with short-circuit and late outcomes | Begun cohort accounted; suppression/fences explicit; result artifact Generated and result testament Posted are terminal by role; no upward recursion |
| Cancel/adopt/expire races with results | Exact effective-state fences; losing late result cannot become accepted truth |
| Crash before/after append, commit, publication and response output | Exact retry and histories; no partially published artifact/testament/claim chain |
| New tables on old/new format boundaries | Old bytes/hash/outcome stable; no invented historic facts; actual decoder floor/activation tests from 18 pass |

Admission limits cover response count, distinct slot/check count, attempts, proof
bytes, lifecycle history, result bundles and pending outputs before allocation.
Counters and indexes support bounded incremental aggregation; do not rescan every
response on every result. Admission fails before an obligation whose completion
cannot fit is committed. Completion/receipt/fence capacity remains reserved under
ordinary pressure, and active required proof cannot be reclaimed by request ACK.
Old terminal records remain durable in the ledger/archive; pruning a hot index must
not erase their exact acceptance/terminal witnesses.

Stage the entire transition set, graph/index changes, command receipt and ordered
deltas under permits before proposal. Publish all at the same prefix only after
commit, or publish none; failure cannot leave half of a response attached. Reads
return each family's recorded state/history plus applicable role and start
eligibility, with bounded, fixed-prefix continuation. An older prefix cannot mix a
new verdict with an old response projection. Context reads expose enough pinned
evidence to invoke a participant's own tool; they neither lease execution nor
reserve a managed mutation ordinal.

L1 does not select numeric tags, successor decoder hashes, snapshot envelopes or
deployment activation commands. Those are concrete L2 implementation decisions in
[18](18-lifecycle-storage-upgrade.md). The transition/authority/aggregation choices
above must be represented there without changing historic behavior. No completed
four-lifecycle CLI/MCP mutation exchange is claimed until L2–L8 implement and qualify
these fixtures.

## 11. Executable contract boundary

The successor [Rust contract](../../crates/focal-model/src/lifecycle/mod.rs)
is independent of `semantics_v1` and the existing stored vocabulary. It deliberately
has no Serde implementation, numeric discriminants, command registration or
activation path. It prepares checked values and transitions. The first native
Core owner publishes creation, posting, Admission entry and control fences in
RAM; durable publication and the remaining family transitions still require
L2–L5. It is not a second live ledger.

| Executable part | Enforced behavior |
|---|---|
| [Claim](../../crates/focal-model/src/lifecycle/claim.rs) | Actor roles, effective revision-bound posting/start predicates, current receipt, first/later response phases, adoption, local completion before graph satisfaction, exact original local sealing position, terminal-cut preservation |
| [Evidence](../../crates/focal-model/src/lifecycle/evidence.rs) | Durable typed production/receipt diagnostics, independent artifact receipt, exact bounded close/attachment plan, separate response post/receive, checked issuer or evaluator entry, immutable terminal projection from aggregate decisions |
| [Validation](../../crates/focal-model/src/lifecycle/validation.rs) | Eleven states, explicit targets and missing slots, private received-manifest capability, designated Actor/definition/generation/receipt/deadline/policy checks, direct agentic and programmatic-plus-quality paths, Error-only retry/fallback, suppression and fences |
| [Owned validation definitions](../../crates/focal-model/src/lifecycle/validation_definition.rs) | Checked allocation plan, independently owned slot and handler policies, reference-free retained evaluation state, exact semantic policy checks across rebinding, acceptance, results and sealed audit membership |
| [Aggregation](../../crates/focal-model/src/lifecycle/aggregation.rs) | Exact same-artifact required-check witnesses, explicit pure Receipt, slot presence declaration order, distinct optional-slot/optional-check effects, incremental coverage across responses, coverage before failure, canonical first cuts and late evidence without repainting |
| [Acceptance manifest and registry](../../crates/focal-model/src/lifecycle/acceptance.rs) | Complete immutable declaration summaries and semantic stamps, exact target/content/generation/receipt registration, separate increment and audit seals, Required Admission and Increment gates, complete pure Receipt proof set |
| [Compact owner registration](../../crates/focal-model/src/lifecycle/registration.rs) | Claim/policy-bound exact evaluation membership, bounded fallible growth and copying, preserved seals; full definitions remain separately owned rather than copied into the registry |
| [Graph](../../crates/focal-model/src/lifecycle/graph.rs) | Complete effective snapshot closure, least fixed point, immutable DependsOn versus Awaits, canonical originating dependency cause, deadline-triggered SCC victim, private revision-bound start/release/failure witnesses |
| [Scope](../../crates/focal-model/src/lifecycle/scope.rs) | Bounded owner-held monitor roots, exact named successor rebind, once-only settlement, atomic child creation/registration, complete owned-child release requirements |
| [Succession](../../crates/focal-model/src/lifecycle/succession.rs) | Compatible actual successor, explicit Supersedes relation, bounded acyclic cause/correction closure, chronology checked on every referring edge, effective read fences, unchanged terminal predecessor |
| [Mandatory creation](../../crates/focal-model/src/lifecycle/creation.rs) | All proposed components checked against actual effective owner lookup; no missing ancestors, duplicate IDs or cycles; same-batch ancestry; atomic child registration and compatible predecessor replacement; public unchecked constructors are test-only |
| [Owned cancellation](../../crates/focal-model/src/lifecycle/ownership.rs) | Root-issuer authority, complete bounded stored child traversal through terminal descendants, exact child identity/content/cause/creation checks, private derived tokens and unchanged terminal cuts/seals; no implicit scope release |
| [Audit](../../crates/focal-model/src/lifecycle/audit.rs) | Actual evaluator result artifacts, exact local sealing prefix, complete bounded attempt history, reserved late-result capacity, begun Observe accounting, suppressed Ready/fence provenance, claimant bundle Generated → Posted without recursive acceptance |

The [two-Actor exchange fixture](../../crates/focal-model/src/lifecycle/exchange_tests.rs)
passes checked capabilities between all four families. It exercises generated but
unposted evidence, closing during an active increment check, distinct claimant
receipt, guarded evaluator-first whole-work entry, typed result evidence,
independent terminal states and an audit bundle closed while the claim still
waits on an actual runtime terminal predicate (Awaits semantics). The contract
executes no external handler.
These are native contract tests, not a claim that the successor CLI/MCP path or
durable publication has been implemented.

Validation definitions own their construction inputs once; retained
`EvaluationState` rows carry no references. Transitions bind a temporary
`Evaluation` view to the exact semantic definition without copying policy buffers.
This removes the self-reference obstacle to owner storage. Its private all-field
stamp is an in-memory consistency guard, not a new durable hash. Construction
and copy APIs report requested bytes, actual capacities and allocation counts.
Core now owns full definitions and evaluation rows under its preparation/page
permits; their durable representation and the remaining transactions stay under
[the storage plan](18-lifecycle-storage-upgrade.md#61-concrete-successor-owner-integration).

The [native Core owner](../../crates/focal-core/src/native.rs) now stages root,
child and successor creation together with every full immutable declaration
required by the acceptance manifest. Missing, extra, duplicate or substituted
definitions are refused. A declared check and a concrete evaluation are distinct:
the declaration owns its policies once, while each registered target/generation
has a separate reference-free `EvaluationState`. A compact `RegistrationSet`
lives beside the claim in its owned row and preserves that complete membership.

`Post` derives a Posted claim and Ready evaluations for all Admission declarations
in one candidate. `BeginAdmission` resolves the retained declaration, evaluation
and registration against the actual claim; readiness, evaluator, policy and
deadline come from those rows and trusted `NativeContext` time. Participants
cannot supply `Passed`, readiness flags or `OwnerState`. A declared
`required_policy` refuses entry until a real stored grant mechanism exists.
Cancellation and supersession find evaluations through the stored registry,
including pending rows, and publish authority fences with the claim control
changes. Terminal evaluations retain their original facts; cancellation does not
release scopes or manufacture respondent testimony.

The owner also implements actual Admission reports with typed attempt provenance,
schema-checked local custody and atomic artifact/result publication. Eligible
begun siblings can report after an ordinary Required failure without changing the
original claim cut. First receipt acquisition consumes checked Admission and graph
start decisions; it creates responsibility, never a testament.

[Respondent evidence transactions](../../crates/focal-core/src/native/work_artifacts.rs)
retain work outputs and diagnostics with immutable claim, receipt, cycle and role
provenance. An owner-held linked cycle index, including exact slot membership,
provides the complete set for
[response closure](../../crates/focal-core/src/native/responses.rs). `CloseResponse`
requires the respondent's explicit summary, confidence and one of the six reported
outcomes; every non-Complete outcome requires real respondent diagnostics. The
attachable manifest includes every Generated/Received work binding; a separate
frozen `failed_work` collection preserves every GenerationFailed/ReceiptFailed row.
Every respondent diagnostic recorded for that cycle is also retained. Diagnostic
and failed-work references never stand in for missing work-slot witnesses.
[FailWorkProduction and RejectWork](../../crates/focal-core/src/native/work_failures.rs)
now establish those failure rows with actual custody and exact producer/target
provenance, without manufacturing a response or changing claim acceptance.
Bounded fallible copying and precharged history growth precede the atomic Generated response, artifact
attachments and claim observation. `PostResponse` and claimant `ReceiveResponse`
remain separate operations. Claimant observation of an unattached work artifact or
an already-posted response can progress after claim terminalization without
changing the claim's state, history or original cut; receipt and identity guards
still apply. Reported success or failure is not an acceptance verdict.

For an open claim, `ReceiveResponse` also materializes the complete declared pure
Receipt cohort from the actual received report and records eligible Pass results
without artifacts, external attempts or an impersonated evaluator. Generation is
the response cycle; authority pins the receipt, exact declaration and report stamp.
At or after a declaration's deadline, receipt is still recorded but its evaluation
remains Ready with no Pass. A late terminal/local-complete claim observation is
for audit only and creates no new evaluation. The same candidate registers the
complete [WholeWork cohort](../../crates/focal-core/src/native/work_checks.rs),
including Required and Observe checks, against actual attached output bindings
or MissingSlot targets. Both cohorts use one precharged registration copy. Work
checks remain Ready even after their deadlines; no attempt, verdict or parent
acceptance is invented. Native WholeWork Begin/report and aggregation remain open.

[Native Increment ownership](../../crates/focal-core/src/native/increments.rs)
now materializes every declared Increment check, including Observe, atomically
with its actual Generated work output. Separate Ready evaluations pin the exact
artifact, receipt and work-cycle generation; work submission invokes no handler.
`BeginIncrement` and `ReportIncrement` resolve those retained rows and the declared
evaluator. The managed owner funds the complete retry/fallback/quality report
chain before Begin. Required failure records the Increment result without
terminalizing the claim or changing artifact lifecycle. Registered checks on
ReceiptFailed outputs retain the independent authority described in §6; closure
and target sealing do not impersonate their evaluator.

Increment results must inherit the target's visibility even when their authored
input list is empty. For work with any Required Increment check, SubmitWork checks
those restrictions against the derived report descriptor and traversal limits
before exposure or custody I/O. This prevents publishing a Required target whose
reports cannot fit. Observe-only work may still be submitted; an unfundable check
cannot begin and does not block the claim. Before funding or rebuilding any grant,
the owner checks the actual source again against the pinned limits, including room
for a content-backed diagnostic. Borrowed registration, work,
evaluation and result reads expose the actual effective or pinned prefix without
copying complete collections. New registration still uses bounded scans per check;
this is not a claim of constant-time cohort insertion or global-scale qualification.

The Admission/Increment completion envelope prices complete response history and
bounded append-only evaluation registration growth, preserving each held grant's original
registration ordinal. Before first responsibility, the owner requires the full
authored target count to fit its registry limits and the combined pure Receipt
and WholeWork cohorts to fit one transaction. Static closing-size checks likewise reject
unrepresentable attachment sets. These guards do not reserve the full respondent
work/closing obligation or its future RAM, disk and replica capacity.

Claims, registrations, definitions, evaluations, artifacts, responses, accepted
results, successful outcomes and typed history share one prepared RAM root. Its
actual effective view includes earlier pending candidates. Publication allocates
nothing; a foreign/stale/out-of-order refusal retains the candidate. Exact pending
retries remain pending and cannot serve as durable acknowledgments. The
[native owner tests](../../crates/focal-core/src/native/tests.rs) exercise this
boundary; qualification results are recorded separately in
[09](09-implementation-status.md). No native codec, restart/import or Session
activation is supplied by this in-process path.

The remaining owner obligations keep L1 open: non-Receipt WholeWork
entry/reporting; deadline/fence publication for expired Ready checks;
receipt adoption; aggregation, graph consequences and audit transactions against
the complete registry. Adoption must derive receipt/definition/generation fences
from actual rows. These additional paths must preserve authorized begun late reports after ordinary required-check failure.
Broader standing and grant policy must likewise resolve actual stored facts;
ingress must never accept participant permission flags. Start and graph release
in the model consume checked proof tokens rather than caller-selected booleans.
Native codecs, WAL/Ready integration, Session activation and CLI/MCP dispatch remain
open; these RAM transactions do not complete the L1–L8 storage rollout.

Acceptance and graph declarations freeze with claim generation. The live owner
must retain the full histories, all simultaneous causes and exact acceptance witnesses, bind
creation/diagnostic terminal facts to commit positions, and publish response,
artifact, validation, claim, graph and request results together under precharged
capacity. The new claim owner has real preparation/page permits and expiring
fixed-prefix RAM reads. Those mechanisms do not by themselves qualify remaining
family buffers, proof bytes, history retention, durable recovery or global scale.
