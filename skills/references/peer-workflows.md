# Peer workflows on the native engine

The peer verbs are authored claims under the owner's rules (architecture
documents 21 §5 and 17 §3, decisions F28 and F29); this reference states the
facts an agent must hold before calling them.

## Vocabulary

- A **challenge** (`action: challenge`) obliges its subject to prove or redo
  stated work. It may `reviews` exactly one committed artifact at its
  descriptor hash (the disputed evidence) and carries an immutable follow-up
  **policy**: `corrective_allowed`, `max_follow_ups` (at most 1024),
  `single_issuer`, `escalation` (`none`, `holder`, `evaluator`).
- A **consultation** (`action: consultation`) asks its subject for work
  answering a query under a quality bar; its optional policy bounds and
  authorizes the follow-ups that `refines` it.
- A **correction** (`action: correction`) `invalidates` exactly one committed
  challenge and `reviews` exactly one artifact: the report of that
  challenge's terminal Fail, Incomplete or Error verdict at the challenge's
  current registration generation.
- A **follow-up consultation** `refines` the consultation it continues.
- `caused_by` keeps its meaning from the child-cause seam: a child of a live
  parent, admitted from the parent's issuer, its current holder (unless the
  parent's policy says `none`) or, under `evaluator`, a designated evaluator
  of the parent's declarations. A terminal or released claim owns no child,
  so corrections and follow-ups link by `invalidates` and `refines` instead
  and never reopen what they follow.

## What the owner checks

| Verb | Admitted only when | Otherwise |
| --- | --- | --- |
| `claim.challenge` | An ordinary authored claim; `policy` present; `artifact` committed at that hash | `invalid_input` (missing policy, refused before any send); `native_refused` `InvalidTarget` (artifact not committed at that hash) |
| `claim.correct` | Challenge is a challenge with `corrective_allowed`; verdict is its terminal negative report at the current generation; author is issuer, holder (escalation ≥ holder) or the reporting evaluator (escalation evaluator); no prior correction under `single_issuer` | `native_refused` with `refusal.kind.Refused` = `InvalidTarget`, `InvalidPolicy`, `MissingEvidence`, `InvalidTransition`, `StaleEvaluation`, `WrongActor` or `ConflictingCause` |
| `claim.follow_up` | Refined claim's policy admits the author; follow-ups so far < `max_follow_ups`; no policy bounds nobody | `native_refused` `WrongActor` or `InvalidPolicy` |

The CLI renders the same refusals as `unauthorized` (exit 3), `invalid_policy`,
`invalid_target`, `missing_evidence` (exit 2), `invalid_transition`,
`stale_evaluation` and `conflicting_cause` (exit 5).
| `claim.consult` | An ordinary authored claim | as `claim.submit` |

The leader applies these rules at admission and every replica at replay;
they read only committed facts of the same ledger.

## Identity

`claim.correct` derives its occurrence from the challenge, the verdict
artifact at its hash and the author; `claim.follow_up` from the refined
claim, the query text and the author. The owner resolves an identical
descriptor to the already committed claim, so duplicate delivery and a lost
reply yield one logical follow-up. A different description or verdict is a
different claim. Supply `occurrence` only to override this deliberately.

## Reading and waiting

`claim.lineage` returns one `native_read` page: `[claim, ancestors…,
followers…]`, followers being the committed corrections, refinements and
children of the claim (at most 64, each with content). `claim.wait` returns
`native_wait`: `condition` (`Met`, `Pending`, `Unmet`), `until`, the latest
`observation` (token, claim, status code, revision, local completion,
release) and `probes`. Neither mints an identity.
