# Focal

Focal coordinates participants through directed claims and an inspectable ledger of evidence and decisions.

## Language

**Participant**:
An identity that issues claims, responds to them, or evaluates their evidence using its own tools and agents.

**Claimant**:
The participant that issues a claim and states its acceptance requirements.
_Avoid_: Requesting worker

**Respondent**:
The participant responsible for answering a claim with a testament and evidence after its work succeeds or fails.
_Avoid_: Automatically launched worker

**Claim**:
A directed request with explicit acceptance requirements and declared dependencies.

**Testament**:
A participant-authored account of a result tied to exact evidence, including failures and errors when they occur.
_Avoid_: Receipt acknowledgment

**Artifact**:
An identified item of evidence, such as a work product, diagnostic, error, or validation report.

**Validation**:
A designated evaluator's check of exact evidence against a stated requirement.

**Owned scope**:
A claim's responsibility for its registered child work and runtime waits, which can remain outstanding after the claim becomes terminal.

**Monitor**:
A named runtime wait for specified claims to become satisfied, terminal, or released.

**Monitor release**:
Successful settlement of every predicate in a monitor's wait.
_Avoid_: Cancellation, channel closure

**Monitor cancellation**:
An explicit claimant decision to end a terminal claim's remaining wait without asserting that the wait's predicates succeeded.
_Avoid_: Successful release, child cancellation

**Monitor disposition**:
The recorded end of a monitor, either successful release or explicit cancellation.

**Owner release**:
Discharge of a terminal claim's owned scope after its child scopes and monitors have been disposed of.
_Avoid_: Claim completion, automatic cancellation

**Monitor deadline**:
A finite limit on a named runtime wait, distinct from the claim's own deadline.
