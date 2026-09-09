# Manual CLI: source research and command coverage

Research date: 2026-09-05. This is a proposed CLI contract and implementation inventory, not a declaration that the proposed commands exist. The current user priority is a complete manual interface for claims, testaments, artifacts, validations, their schemas, and cluster setup. Command spellings below are recommendations for Focal; neither reference repository supplies an exhaustive shell grammar to copy.

The research inspected Hecate revision `103c0785d2623c19d0c02a450e94677bbfc70359`, Sylk revision `50154e6159c7ed590728b82423dde3e7fc977c26`, and the current Focal working tree. Focal's working tree contains implementation beyond its HEAD, so its source links describe the inspected tree, not a release. No sibling code was changed or sibling tests run. Hecate citations use the checked-in frozen reference snapshot; Sylk citations link the inspected sibling source. The primary evidence is repository source; no external product claims are needed for this comparison.

**Historical inventory:** the source matrices and proposed spellings below record the 2026-09-05 baseline, including statements that commands were then absent. They are not the current command list. The domain CLI/MCP, optional-filter queries, transfers, recovery, named remote contexts and substantial cluster administration have since landed; use [the current interface contract](19-cli-mcp-implementation.md), [manual guide](../manual-cli.md) and [status record](09-implementation-status.md). In particular, durable registration is `monitor register`, while `claim wait` is a separate bounded read; Focal does not implement the historical `validator evaluate` proposal or execute participant tools. Independent lifecycle, challenge/consult and deployment completion remain open.

## 1. Findings that determine the interface

1. **Expose the four ledger object families explicitly.** Claims describe obligations; testaments close an evidence set on success or failure; validations judge a claim; artifacts are immutable typed evidence. A validation is not a validator implementation, and an artifact kind is not its payload schema. Hecate states these boundaries in [LEDGER.md §1–2](reference/hecate/docs/architecture/LEDGER.md#L18). Focal represents them in [objects.rs](../../crates/focal-model/src/objects.rs#L30).
2. **Lists must work without a source, parent, lifecycle, or actor filter.** Optional filters narrow a bounded list. Sylk has both general lifecycle queries and relationship-specific queries; its tools are not evidence that every list requires a claim ID. See `QueryBoardSkill` in [skills.go](../../../sylk/core/claims/skills.go#L48) and the query methods in [board.go](../../../sylk/core/claims/board.go#L2018).
3. **“Get by source” needs an explicit meaning and cardinality.** Neither Hecate's ledger specs nor Sylk's claims package declares a universal `GetBySource`. Focal's proposed ergonomic meaning is claim issuer: `--source` aliases `--issuer`, while `--target` aliases subject. This can match multiple claims, so a singular `get claim` must reject ambiguity. Scope, causal parent, artifact input, URI, and content hash use separately named selectors. Section 5 explains this deliberate Focal choice.
4. **Flags and JSON/YAML must build the same typed request.** They are input presentations, not independent command implementations or different authorization paths. Hecate's single-binary ADR requires the same protocol over local and remote bindings; its wire spec separates canonical encoding from tool-schema projections. Focal currently uses explicit Rust types, Postcard framing, and a separate authored-content identity encoder. See [ADR-0004](reference/hecate/docs/adr/0004-single-binary-with-client-runtime-seam.md), [WIRE_FORMAT.md §5](reference/hecate/docs/specs/WIRE_FORMAT.md#L133), [frame.rs](../../crates/focal-wire/src/frame.rs#L34), and [canonical.rs](../../crates/focal-model/src/canonical.rs#L1).
5. **Convenience commands must preserve lifecycle and authority.** Creating a claim does not post it; a generated testament is not validation success; a local validator result is not a committed verdict. Client input cannot claim Runtime authority, a peer principal, logical time, or durable custody. These are explicit constraints in [Hecate LEDGER.md §2–3](reference/hecate/docs/architecture/LEDGER.md#L90), [Command](../../crates/focal-model/src/command.rs#L1), and [VerifiedRequest::into_authenticated](../../crates/focal-wire/src/auth.rs#L199).
6. **Cluster setup and stronger application protection are separate milestones.** Focal's current invite/join workflow enrolls nodes and admits root metadata learners. It does not place application replicas, promote root voters, or strengthen application evidence custody. The manual must make that visible, while exposing only the next necessary choices. See the implemented [network startup guide](../network-startup.md) and the target [stepped-complexity contract](08-stepped-complexity-and-deployment.md#1-the-progression-contract).

## 2. What each repository actually provides

| Evidence | Implemented or specified behavior | Consequence for Focal |
|---|---|---|
| Hecate [LEDGER.md](reference/hecate/docs/architecture/LEDGER.md), especially §2–5 | Four objects; immutable authored content; typed relations; durable lifecycle; graph reads | Domain authority for the manual, not a shell command reference |
| Hecate [LEDGER_CORE.md §2–5](reference/hecate/docs/specs/LEDGER_CORE.md) | Durable acknowledgment before apply, bounded graph queries, validator execution outside the ledger owner, cursor replay/resync | The CLI must report durable outcomes and use bounded reads and streams |
| Hecate [WIRE_FORMAT.md §3c–6](reference/hecate/docs/specs/WIRE_FORMAT.md) | Inline byte limits; content references; strict decoding; generated reflection/schema projection; append-only compatibility | A human document must lower into a versioned type and preserve size/evolution constraints |
| Hecate [ADR-0004](reference/hecate/docs/adr/0004-single-binary-with-client-runtime-seam.md) | One binary; real protocol over in-memory, Unix, and remote bindings | Changing deployment should change connection inputs, not object commands |
| Sylk [cmd/root.go](../../../sylk/cmd/root.go#L10) | Cobra root starts an interactive TUI | There is no existing comprehensive ledger shell CLI to port |
| Sylk [skills.go](../../../sylk/core/claims/skills.go#L48) | Agent tools `query_board`, `post_action`, `submit_testaments` | Useful operation coverage and examples, but not an established CLI grammar |
| Sylk [skills_context_queries.go](../../../sylk/core/claims/skills_context_queries.go) | Ancestry, action claims/causality, overlapping scopes, validation history, all testaments for a claim, artifact recall, phase history | These inform filtered reads and graph traversal; Focal need not preserve Sylk-only phase/action records |
| Sylk [skills_traverse.go](../../../sylk/core/claims/skills_traverse.go#L16) | Node traversal with optional relationship filter and depth | Prefer a shared traversal query behind convenience reads |
| Sylk [type_registry.go](../../../sylk/core/claims/type_registry.go#L55), [validator_registry.go](../../../sylk/core/claims/validator_registry.go#L22) | Typed artifact codecs and registered validator contracts with timeout/concurrency metadata | Schema inspection and validator inspection are separate surfaces |
| Focal [main.rs::Commands](../../crates/focal-node/src/main.rs#L53) | `start`, `cluster invite`, `join`, `demo`, `status`, `request FILE`, `identity`, `deployment explain`, `deployment schema` | These are the current shell commands; all new ledger-family commands below remain work |
| Focal [message.rs::ReadQuery](../../crates/focal-wire/src/message.rs#L31) | Typed point reads, unfiltered typed-key scan, depth-bounded traversal | Server-side family/source/lifecycle filters and traversal edge filters are not yet public wire queries |
| Focal [command.rs::Command](../../crates/focal-model/src/command.rs#L25) | Twenty-nine append-numbered domain mutations | The manual can cover the whole implemented mutation vocabulary without inventing generic CRUD |

The source conflicts matter. Sylk's `submit_testaments` example uses `low/medium/high`; Hecate and Focal use `hint/tentative/committed/consensus`. Sylk's artifact has a string `Reference`, string `DataType`, and metadata map; Focal has a schema hash, typed payload, content reference, and typed inputs. Sylk's full projection tool is deprecated as expensive, some query limits allow zero to mean “all,” and its traversal description advertises depth zero while its handler converts `maxDepth <= 0` to one. None of these quirks should become Focal's new contract. Sources: [Sylk skills.go](../../../sylk/core/claims/skills.go#L26), [Sylk Artifact](../../../sylk/core/claims/types.go#L897), [Sylk traversal handler](../../../sylk/core/claims/skills_traverse.go#L54), [Focal vocabulary](../../crates/focal-model/src/vocabulary.rs#L60).

Hecate's specific `#[derive(Wire)]`/no-Serde implementation prescription is also not an accurate description of Focal today. Adopt the single-source schema/evolution objective deliberately; do not claim that copying the spec has implemented its codec or silently change Focal's persisted encodings while adding CLI input formats.

## 3. Schema and document model

The CLI should expose the following distinct schema layers. Every schema response must name the layer, version/hash, media/encoding expectations, and bounded payload size. A schema hash identifies exact bytes or a precisely defined canonical schema; it is not the artifact's descriptive kind.

| Layer | Actual source fields | Proposed manual surface | Current gap |
|---|---|---|---|
| Claim document | `NewClaim`, `ClaimContent`, embedded `NewValidation`; relations, scopes, requirement specification hashes | `schema get claim`; `schema validate claim --file FILE`; `submit claim` | Rust types exist; generated machine-readable object schemas and friendly input builder do not |
| Testament closure document | Claim/receipt fence, evidence-set ID, exact artifact ID/hash manifest, summary, confidence, outcome | `schema get testament-close`; `submit testament` | Closure command exists; no named CLI or schema export |
| Artifact envelope | Open `kind`, `schema_hash`, metadata bytes, payload, producer, receipt, typed inputs, visibility | `schema get artifact`; `artifact attach` | Envelope exists; kind is not a closed schema registry |
| Artifact payload | Schema hash plus actual payload bytes; `ContentRef` for bulk content | `schema list --category artifact-payload`; `schema get --hash HASH`; `schema validate --hash HASH --file FILE` | No public catalogue/retrieval RPC; arbitrary hash does not mean its schema is installed |
| Validation specification | Kind, phase, mode, description, quality bar, evaluator, pinned handlers, allowed evidence schemas, provenance, policy revision | `schema get validation`; definition within claim input; `get validation ID` | No standalone create/update-validation command; claim generation owns the specification |
| Validator contract | Validator ID, implementation version hash, agentic capability, evidence schema, byte limit; runtime execution policy separately | `validator list`; `validator get ID --version HASH` | Registry/dispatch exist in code; no remote enumeration or operator installation protocol |
| Wire request/result | `RequestEnvelope`, operation, response envelope, typed receipts and errors | `schema get request`; `request build/check/send` | Existing `request FILE` parses JSON only; no complete schema projection |
| Deployment policy | Versioned `Settings`, topology, durability, placement; distinct runtime facts | Existing `deployment schema`; planned `deployment config show/validate` | Schema exists for deployment, not for the whole ledger |

The field evidence is in [objects.rs](../../crates/focal-model/src/objects.rs#L100), [message.rs](../../crates/focal-wire/src/message.rs#L171), [validators.rs](../../crates/focal-evidence/src/validators.rs#L32), and [config.rs](../../crates/focal-node/src/config.rs#L9). Sylk's registration validates concrete input/output types against its artifact registry; `ListArtifactTypes` and `ValidatorRegistry.List` are code APIs, not proof of an implemented JSON Schema endpoint. See [type_registry.go](../../../sylk/core/claims/type_registry.go#L96) and [validator_registry.go](../../../sylk/core/claims/validator_registry.go#L93).

An existing concrete payload example is Focal's test report: `{"passed":1,"failed":0,"skipped":0}`. Its pinned contract rejects unknown fields, reports failure when `failed > 0`, and reports incomplete when there are no passing executed tests. The schema identifier comes from `test_report_schema()`, not the free-form string `test-output`. This example must retrieve or generate its exact current hash rather than hardcode an invented value. See [validators.rs::TEST_REPORT_SCHEMA and TestReportValidator](../../crates/focal-evidence/src/validators.rs#L5).

The proposed schema catalogue is read-only introspection. `validators install`, executing arbitrary shell validators from a document, or replacing a pinned implementation is a separate trusted deployment capability and is not implied by supporting `validator list/get`.

### 3.1 Authored flags and document fields

These are proposed fields of the human input DTO, lowered into existing Rust types. They do not change the binary representation of existing types. All generated IDs, chosen defaults, requirement hashes and transport context are materialized in the saved expanded operation before transmission. Fresh independently authored invocations may generate different IDs; flags/JSON/YAML parity compares the same expanded operation, not independently generated occurrences.

| Claim flag / authored document field | Rust destination | Requirement or default |
|---|---|---|
| `--id` / `id` | `NewClaim.id` | Optional on a fresh operation; allocate once and journal; explicit ID must be valid/nonzero |
| `--occurrence` / `occurrence` | `ClaimContent.occurrence` | Allocate once if absent; never regenerate on retry |
| Selected context / optional explicit ledger selection | `ClaimContent.ledger` and envelope ledger | Default from authorized context; every embedded object must match |
| `schema_version` | `ClaimContent.schema` | Builder's supported pinned version, recorded in expanded input; unsupported version rejected |
| `--description` / `description` | `ClaimContent.description` | Required bounded text; no empty fabricated obligation |
| `--target` / `target` | `RelationKind::Subject → Participant` | Required subject participant; resolve names to exact IDs before journaling |
| Authenticated actor | `RelationKind::Issuer → Participant` | Derived; ordinary input cannot impersonate an issuer; read `--source` is unrelated |
| `--action` / `action` | `RelationKind::ClaimAction → ActionType` | Default ordinary `Work` if omitted; explicit supported action pinned in saved input |
| Trusted causal context / authorized parent intent | `RelationKind::CausedBy → Root or Claim` | Use real ingress root; child-cause support needs the owner-validated seam in P17, not a forged `--cause` |
| `--evaluator` / evaluator relation | `RelationKind::Evaluator → Participant` | Resolve explicit choice or an actual authorized context default; validation evaluator fields must agree with the intended contract |
| Repeat `--scope KIND:KEY` / `scopes` | `ClaimContent.scopes` | Empty only where legal; closed kind, bounded exact key; no inferred file scope from description |
| Typed relation flags / `relations` | `ClaimContent.relations` | Optional validated set; issuer/subject/action/cause cannot conflict with derived fields; qualified object targets retain ledger and kind |
| Repeat `--validation-file` / `validations` | `NewClaim.validations` | Explicit bounded specification set or a named, visible validated template; do not invent passing acceptance criteria |
| Derived requirement references | `ClaimContent.requirements` | Compute each validation ID and immutable specification hash from the exact supplied definitions; preserve ordering; reject conflicting supplied hashes |
| `--deadline` / `deadline` | `Option<Deadline> {timer,generation,at}` | Optional when legal; resolve human time under documented clock semantics, generate timer fence once; do not reinterpret it as server logical-time authority |

Each embedded validation definition covers **all** of `NewValidation.id` and `ValidationContent {ledger, schema, claim, kind, phase, mode, description, quality_bar, evaluator, handlers, evidence_schemas, contributed_by, policy_revision}`. Generate its ID once if absent; derive ledger/claim from its parent; pin schema/policy context. Require the actual kind, phase, mode, description and evaluator contract rather than filling arbitrary success defaults. Quality bar is optional. Handlers carry exact ID/version/agentic capability; evidence schemas are exact hashes. Provenance is descriptive and checked; it does not grant contributor authority. The receipt-only special case must satisfy the real core rules, not be used to bypass quality requirements. Complex fields belong in the same JSON/YAML DTO even if their flag syntax uses repeated typed values or a bounded subdocument. Source: [NewValidation and ValidationContent](../../crates/focal-model/src/objects.rs#L106).

| Testament flag / authored document field | Rust destination in `CloseTestament` | Requirement or default |
|---|---|---|
| `--claim` / `claim` | `claim` | Required exact claim; use selected ledger |
| `--receipt` and `--receipt-epoch` / `receipt` | `ReceiptFence {receipt,epoch}` | Required fence, or exact saved acquisition result; never silently adopt another receipt |
| `--evidence-set` / `evidence_set` | `evidence_set` | Required opened set, or saved same-workflow begin result |
| `--id` / `id` | `testament` | Allocate once if omitted; journal before send |
| Repeat `--artifact ID:HASH` or `--manifest-file` / `manifest` | `Vec<ArtifactRef>` | Exact bounded ordered ID/hash list; derive only from this workflow's durably attached artifacts; never enumerate unrelated claim evidence |
| `--summary` / `summary` | `summary` | Required bounded text |
| `--confidence` / `confidence` | `Confidence` | Explicit `hint`, `tentative`, `committed`, or `consensus`; do not translate legacy low/medium/high silently |
| `--outcome` / `outcome` | `OutcomeKind` | Explicit complete/partial/refused/impossible/interrupted/failed; negative outcomes still close with evidence |
| Selected context and supported schema | Resulting `TestamentContent.ledger/schema` | Fixed by the actual command/model contract; no client-supplied lifecycle fields |

The proposal must distinguish input shape validation from domain admission. For example, `focal submit claim --json '{}'` and `focal submit testament --yaml '{}'` select the correct parser and return required-field diagnostics; they do not create valid empty records. The existing low-level command remains `focal request FILE`. Field sources: [ClaimContent/NewClaim](../../crates/focal-model/src/objects.rs#L30), [CloseTestament](../../crates/focal-model/src/command.rs#L76), and [TestamentContent](../../crates/focal-model/src/objects.rs#L178).

## 4. Proposed exhaustive command matrix

Status legend:

- **C**: named shell command exists in the inspected implementation.
- **W**: underlying typed operation exists; this named shell interface does not.
- **P**: requires an additional query, schema, or service contract, not just argument parsing.
- **I**: intentionally restricted internal/operator/evaluator operation; no ordinary actor grant is implied.

Every row is scoped by an authenticated connection and ledger where appropriate. The singular IDs below are typed IDs, never guessed from an untyped global ID namespace. The `submit/get/list` grammar follows [13](13-cli-and-agent-implementation-plan.md); names remain proposed until implemented. Object lifecycle verbs are grouped under their object. Low-level entries marked `admin` below describe a restricted operator disposition, not an additional shipped root command.

### 4.1 Reads, traversal, schemas, and streams

| Proposed command | Required selector | Optional selectors/behavior | Status and backend |
|---|---|---|---|
| `list claims` | None | `--source`/`--issuer`, `--target`/`--subject`, `--evaluator`, `--action`, `--status`, `--scope KIND:KEY`, `--caused-by`, `--relation KIND:TARGET`, `--limit`, `--cursor` | P: filtered bounded query; current scan alone is not a filtered index |
| `get claim ID` | Claim ID | Consistency token; optional bounded related-object expansion | W: typed point read; expansion needs separately bounded traversal |
| `list testaments` | None | `--claim`, `--outcome`, `--confidence`, `--status`, pagination | P; `--claim` must remain optional |
| `get testament ID` | Testament ID | Exact artifact references; payload download separate | W: point read |
| `list artifacts` | None | `--claim`, `--testament`, `--producer`, `--kind`, `--schema-hash`, `--input KIND:ID`, pagination | P: structural and provenance predicates must have defined index semantics |
| `get artifact ID` | Artifact ID | Envelope/metadata; optional verified payload download to explicit output | W: point read plus content RPC when referenced |
| `list validations` | None | `--claim`, `--evaluator`, `--kind`, `--phase`, `--mode`, `--status`, `--validator`, pagination | P; avoid making a parent claim mandatory |
| `get validation ID` | Validation ID | Specification plus planned execution/target/verdict expansion | W for specification point read; P for execution runs and verdict/result read surface |
| `get claim --source PARTICIPANT [--target PARTICIPANT]` | Claim issuer filter; subject optional | Require exactly one authorized match; no match is not found; multiple matches are ambiguous | P: bounded singular filtered read from §5 |
| `ledger traverse KIND:ID` | One or bounded multiple typed roots | `--edge KIND`, `--depth`, `--limit`, continuation | W for roots/depth; P for edge predicates and full continuation contract |
| `claim history ID`, `validation history ID` | Typed object ID | Bounded lifecycle/relationship history, sequence window | P: define retained history query; do not synthesize history from only the current row |
| `ledger summary` | Ledger | Bounded counters with read token and documented scope | P: no full graph download to count client-side |
| `ledger watch` | Ledger and stable consumer identity | Object/delta filter, resume cursor, item/byte credits, seed choice | W: typed `Stream::Open/Poll/CompleteSeed` |
| `ledger cursor show` | Saved local cursor file | Consumer, generation, scope, exact position and resolved marker | P: local presentation; cursor is opaque authority-bound data |
| `ledger cursor resume` | Saved cursor file | Output sink, explicit completion acknowledgment policy | W/P: stream RPC exists; durable CLI sink/cursor journal required |
| `schema list` | None | Category, object family, version; bounded remote catalogue where applicable | P |
| `schema get NAME` or `schema get --hash HASH` | One unambiguous selector | JSON Schema or exact registered representation; origin/version | P except existing deployment-specific schema |
| `schema validate NAME --file FILE` | Exact schema + document | JSON/YAML input; no mutation; diagnostics with field paths | P: local syntactic/structural validation only |
| `validator list` | None | Kind, accepted schema, agentic capability, installed version | P: bounded server introspection |
| `validator get ID --version HASH` | Pinned validator | Contract, inputs/results, execution policy and availability | P |
| `validator evaluate ID --version HASH --file FILE` | Pinned validator and input | Explicit offline mode; no ledger verdict; bounded execution | P/I: requires a defined safe execution surface, not just schema validation |

Sources for the read coverage are [Sylk QueryBoardSkill](../../../sylk/core/claims/skills.go#L48), [context query skills](../../../sylk/core/claims/skills_context_queries.go), [Hecate graph contract](reference/hecate/docs/architecture/LEDGER.md#L149), and [Focal ReadQuery/StreamRequest](../../crates/focal-wire/src/message.rs#L31). Filters that have no corresponding Focal field/index must be implemented explicitly or rejected as unsupported; the CLI must never accept and ignore them.

### 4.2 Actor mutations and the complete domain command vocabulary

All named mutation commands below are unimplemented CLI surfaces. The table covers every current `Command` tag, including commands that should remain behind a restricted namespace. “Actor” means eligible for actor ingress, still subject to issuer/subject/evaluator/receipt and lifecycle checks; it does not mean any actor may mutate any object. Source: [Command::code](../../crates/focal-model/src/command.rs#L179) and [wire capability classification](../../crates/focal-wire/src/auth.rs#L148).

| Tag / typed command | Proposed spelling | Inputs and boundary | Status/role |
|---|---|---|---|
| 1 `NegotiateEpoch` | `request epoch open` | Admit the authenticated principal's epoch through `Operation::OpenEpoch`; no arbitrary client principal | W: safe own-principal operation; raw command Runtime-only |
| 2 `AdvanceEpochFloor` | `admin requests advance-floor` | Minimum retained epoch; explain loss of old retry coverage | W/I Runtime |
| 3 `GenerateClaim` | `submit claim` | Claim content and embedded validation definitions; creates Generated state only | W Actor |
| 4 `GenerateClaimBatch` | `submit claim --file FILE` with a batch document | Bounded atomic batch; complete generated IDs and exact input journal | W Actor |
| 5 `PostClaim` | `claim post ID` | Existing Generated claim; do not silently combine creation and posting | W Actor |
| 6 `AcquireReceipt` | `receipt acquire ID` | Receipt ID and epoch; return exact receipt fence | W Actor |
| 7 `AdoptReceipt` | `admin claims adopt-receipt ID` | Previous fence, new receipt, holder and epoch | W/I Runtime |
| 8 `RecordProgress` | `claim progress ID --message TEXT` | Current receipt fence; progress never completes work | W Actor |
| 9 `BeginEvidenceSet` | `evidence begin --claim ID` | Receipt fence and evidence-set ID; return reusable identifiers | W Actor |
| 10 `AttachArtifact` | `artifact attach --claim ID --evidence-set ID` | Receipt fence, immutable artifact envelope, verified payload reference | W Actor |
| 11 `CloseTestament` | `submit testament --claim ID --evidence-set ID` | Receipt fence, testament ID, exact manifest, summary, confidence and outcome | W Actor |
| 12 `AcknowledgeTestament` | `admin testaments acknowledge ID --claim ID` | Runtime delivery acknowledgment, not satisfaction | W/I Runtime |
| 13 `BeginWholeWorkValidation` | `admin validations begin --claim ID --phase whole-work` | Starts runtime-owned validation work | W/I Runtime |
| 14 `BeginIncrementValidation` | `admin validations begin ID --phase increment` | Claim, pinned target and manifest hashes | W/I Runtime |
| 15 `RecordValidationVerdict` | No new convenience verb | Legacy unfenced path; retain decoding compatibility, prefer tag 29 for new clients | W/I Evaluator |
| 16 `CompleteWholeWork` | `admin claims complete ID` | System transition after satisfaction preconditions; not a force-success switch | W/I Runtime |
| 17 `FailPost` | `admin claims fail-post ID` | Durable error artifact reference | W/I Runtime |
| 18 `FailReceipt` | `admin claims fail-receipt ID` | Durable error artifact reference | W/I Runtime |
| 19 `FailTestamentGeneration` | No new CLI surface | Historical runtime-synthesized testimony only; new admission refuses it. Respondents submit ordinary testaments with error evidence. | Replay only; supersedes the original Runtime proposal |
| 20 `CancelClaim` | `claim cancel ID --reason TEXT` | Authorized cancellation, recorded lifecycle | W Actor |
| 21 `RevokeClaim` | `admin claims revoke ID --reason TEXT` | Trusted lifecycle change | W/I Runtime |
| 22 `ExpireClaim` | Internal timer dispatch; optional operator diagnostic | Exact timer, generation and fired time; no client-selected logical clock | W/I Runtime |
| 23 `SupersedeClaim` | `claim supersede ID --file SUCCESSOR` | Immutable successor claim and its validations, relation and standing checks | W Actor |
| 24 `RegisterMonitor` | `claim wait ID` | Bounded typed predicates, monitor ID and deadline | W Actor; server/runtime completion follows |
| 25 `RebindMonitor` | Internal monitor repair | Predecessor/successor identity | W/I Runtime |
| 26 `ReleaseScope` | `admin claims release-scope ID` | Lifecycle-permitted release; no direct scope-table editing | W/I Runtime |
| 27 `RegisterArtifact` | `admin artifacts register --file FILE` | Trusted standalone/result artifact registration; ordinary actor uses attach | W/I Runtime |
| 28 `ExpireMonitor` | Internal timer dispatch | Exact timer fence | W/I Runtime |
| 29 `RecordFencedValidationVerdict` | `validation record-verdict ID --file FILE` | Exact target, handler/version, verdict, and current optional receipt fence | W/I Evaluator |

There is no generic `claims update`, `testaments edit`, `artifacts overwrite`, `validations delete`, or direct `set-status`. A later validation-set change requires an explicit supported domain transition or a successor claim; it must not mutate immutable requirement specifications behind a friendly CLI. Relations such as `amends` exist in the vocabulary, but that alone does not establish a standalone `testaments amend` command or permission to rewrite a terminal testament. Sources: [Hecate writer disjointness](reference/hecate/docs/architecture/LEDGER.md#L98), [Focal command variants](../../crates/focal-model/src/command.rs#L25), and [vocabulary.rs](../../crates/focal-model/src/vocabulary.rs#L60).

For ergonomic `submit claim` flags, provide at least `--id`, `--description`, `--target`, `--action`, repeatable `--scope`, typed dependency/relation flags, and validation-definition inputs. Use `--target` as the ergonomic subject flag; `--source` is an issuer read filter, not an authorship override. Claim issuer defaults to the authenticated identity; a server-derived causal parent must match real ingress authority. Complex validation sets may use `--validation-file` or the full document form. Never accept a client-supplied `--runtime`, `--durable`, `--schema-valid`, `--logical-time`, or unrestricted `--principal` as authority. Immutable root/issuer defaults are context, not permission elevation.

### 4.3 Content transfer and request recovery

| Proposed command | Required inputs / result | Status |
|---|---|---|
| `content upload FILE` | Hash/length/class computed or checked locally; durable upload journal; stream bounded chunks; seal returns `ContentRef` only after configured custody gate | W: `Upload::Begin/Append/Seal` |
| `content upload --resume JOURNAL` | Reuse upload ID and exact offsets/bytes; reconcile acknowledged prefix | W/P: RPC exists, CLI journal does not |
| `content cancel --upload ID` | Authenticated ledger/principal scope; does not delete attached immutable evidence | W: `Upload::Cancel` |
| `content download --ref FILE --output PATH` | Exact typed content reference, offset resume, digest verification, bounded writes | W: `Download` |
| `artifact download ID --output PATH` | Resolve artifact at a read prefix, then verified content transfer or bounded inline output | W |
| `request FILE` | Existing full JSON `RequestEnvelope`; same file on retry | C: local Unix transport today |
| `request build OP ... --output FILE` | Flags/document → typed complete request, fixed IDs and chosen schema/version; no send | P |
| `request check FILE` | Strict parse and structural checks; output exact normalized request/hash; no admission claim | P |
| `request send FILE` | Alias/evolution of existing `request FILE`; identical semantics and retry identity | W |
| `request retry JOURNAL` | Replay exact stored typed intent; report committed, rejected, expired history, or unknown distinctly | P: builds on client retry support |
| `request receipt KEY` | Authoritative retained receipt lookup for the appropriate data/control request namespace | P: control has `ControlRead::Receipt`; ordinary data wire needs an explicit read contract |

Do not collapse a `ContentRef` into a bare hash: domain, content class, length and root participate in identity and authorization. The upload ID alone is not a capability. Upload sealing and testament closure are different commits. Sources: [ContentRef](../../crates/focal-model/src/objects.rs#L134), [UploadRequest/UploadReply](../../crates/focal-wire/src/message.rs#L265), and [ControlRead](../../crates/focal-control/src/rpc.rs#L5).

### 4.4 Process, connection, cluster, and deployment

| Command or command family | Existing scope or proposed contract | Status |
|---|---|---|
| `context list/show/use` | Select a bounded saved authenticated connection and default ledger; endpoint selection does not change claim subject | P |
| `context add/remove` | Save or remove client connection metadata safely; this is not node enrollment or cluster membership | P |
| `completion SHELL` | Generate shell completion from the actual command registry, never secret values | P |
| `start [--advertise ENDPOINT] [--listen ADDRESS]` | Foreground durable service; saved identity/addresses on restart; omit network inputs for local use | C |
| `identity` | Read saved identity metadata without acquiring ledger ownership | C |
| `status` | Current implementation reads the application ledger's authoritative prefix; not root-learner health | C; richer readiness/placement status P |
| `demo` | Existing end-to-end example with exclusive local ownership; not the sole way to operate the ledger | C |
| `cluster invite --node NAME --output FILE` | Running founder's authenticated local admin socket; exact durable invitation label and private output | C |
| `join --invite-file FILE --advertise ENDPOINT [--listen ADDRESS]` | Persist pinned credentials and physical identity, then exit; `start` is separate | C |
| `cluster status` | Requested/effective/observed protection, root and directory readiness, bounded node counts | P: separate from application prefix status |
| `cluster nodes list/get` | Bounded committed contacts, credential state, membership role, catch-up state, known/unknown topology | P; do not present contact registration as voter admission |
| `cluster invitations list/get/revoke` | Metadata-only inspection, exact ID and expiry; never display secret; revoke committed admission authority | P: CLI absent; enrollment primitives are not a finished admin API |
| `cluster credentials renew` | Durable key/CSR identity; the same key under a fresh certificate, interrupted renewal reconciles on the committed one (2026-09-09); key rotation with a proof of the previous key remains planned | C |
| `cluster nodes drain/remove` | Plan and commit handoff/removal; maintain placement guarantees; distinguish initiation from completion | P: internal membership primitives exist, operator workflow incomplete |
| `cluster membership show` | Explicit group-scoped configuration and applied fence | W/P: `ControlRead::Membership/Configuration`, not an existing command |
| `cluster membership add-learner/promote/remove/leave-joint` | Restricted repair/admin surface, expected configuration and stable operation ID; report committed configuration | W/I: internal control/session APIs exist; do not require users to operate raw Raft for normal growth |
| `cluster leader transfer` | Trusted group-scoped initiation; separately observe resulting leader; never print “committed” for accepted transfer | W/I: `ControlRpc::Transfer` |
| `cluster endpoint change` | Authenticated, durable endpoint transition bound to saved physical identity | P; current startup rejects changed saved addresses |
| `deployment schema` | Emit current machine-readable deployment settings schema | C |
| `deployment explain [--inventory FILE]` | Existing offline placement solver; does not activate a guarantee | C |
| `deployment capabilities` | Supported versions, measured resources and qualified features; missing facts stay unknown | P: target in architecture §9 |
| `deployment config show/validate` | Explain effective configuration and source of each value; validation does not mutate policy | P |
| `deployment plan --config FILE` | Immutable plan bound to deployment, current revisions, actual topology, and desired policy; effects/limits explicit | P |
| `deployment apply --plan FILE` | Recheck exact preconditions, durably journal work, report achieved/blocked transitions | P |
| `deployment render --target kubernetes` | Packaging for the same binary/membership; stable storage/identity and resource inputs | P; no Kubernetes prerequisite for VMs or local use |
| `deployment progress/get-plan` | Bounded durable progress and outstanding unknown outcomes | P |

Current grammar is [main.rs](../../crates/focal-node/src/main.rs#L53); current limitations and exact retry behavior are [network-startup.md](../network-startup.md#restart-and-retry). The proposed operator behavior follows [08 §2 and §9](08-stepped-complexity-and-deployment.md). Membership reads and transfer replies are typed in [rpc.rs](../../crates/focal-control/src/rpc.rs#L5).

The progression must not introduce `--mode laptop|vm|kubernetes|global`, raw shard-count configuration, or manual PKI as the ordinary next step. Local needs a data directory; first networking needs a reachable endpoint plus a private invitation; Kubernetes adds packaging/storage inputs; failure-domain protection adds verified domain facts and the requested number of failures; regions add residence and placement constraints. Those are desired interface properties from [08 §1](08-stepped-complexity-and-deployment.md#1-the-progression-contract), not proof that every stage is implemented. The CLI must refuse or explain an unsatisfied stronger policy rather than relabel local durability as replicated durability.

Hecate's VMM, agent roster, VFS, model/provider configuration, and Sylk's MCP/TUI commands are outside this ledger CLI scope. Claims can describe work for those systems without Focal implementing their administration interfaces.

## 5. Optional filters and “get by source”

### 5.1 Proposed issuer selector and distinct provenance queries

The grammar in [13](13-cli-and-agent-implementation-plan.md) chooses `--source PARTICIPANT` as a claim issuer filter and `--target PARTICIPANT` as a subject filter. This is an ergonomic Focal proposal derived from actual issuer/subject relations, not an upstream command spelling. Neither flag changes transport context or impersonates another participant. Preserve the following distinctions:

| User intent | Proposed selector | Result/cardinality and required backend |
|---|---|---|
| Claim issuer, the source of directed work | `--source PARTICIPANT` / `--issuer PARTICIPANT` | Bounded claims; singular filtered `get claim` requires exactly one match |
| Claim subject, the target of directed work | `--target PARTICIPANT` / `--subject PARTICIPANT` | Bounded claims; unrelated to a connection endpoint |
| Object that evidence was derived from | `--input KIND:ID` for artifact inputs, or `--derived-from KIND:ID` for an explicit relation | Bounded zero-to-many objects; exact typed provenance index |
| Claims originating under a causal parent | `--caused-by KIND:ID` or typed root identity | Bounded claims; causal relation semantics, not any matching input |
| Work affecting a source file/symbol/API | `--scope KIND:KEY` | Bounded matching claims; define exact versus overlap semantics explicitly |
| Testaments responding to a claim | `--claim ID` | All matching retained testaments, including superseded ones unless filtered |
| Artifact structural parent | `--testament ID` | Bounded artifacts in the exact testament's manifest |
| Artifact payload identity | `--content-ref FILE` or an explicitly scoped content-root selector | Content identity lookup, not provenance |
| External URI, repository revision/path, or tool output source | Future `SourceRef` schema with explicit normalization/version rules | Not currently a Focal indexed field; reject unsupported source kinds |


Use `get claim --source PARTICIPANT [--target PARTICIPANT]` for the requested get-by-source workflow: zero authorized matches → not found; exactly one → that claim; more than one → ambiguous with a bounded diagnostic suggesting `list claims`. No implicit “latest” or first-result selection is allowed. The bounded scan must establish uniqueness across its exact read prefix before returning singular success; finding one match before exhausting the authorized search does not suffice. If the scan budget cannot establish uniqueness, return continuation/insufficient-query status and direct the user to narrow the query or use a list. Positional IDs and filtered selectors are mutually exclusive input modes.

For other families, use the actual relationship name rather than giving `--source` a second undocumented meaning. `ArtifactContent.inputs` directly supports typed input identity in the model; other lineage queries may require a relationship walk or added index. Provenance filtering follows one declared edge by default; recursive discovery belongs to bounded traversal. Do not reinterpret an ID typo as a URI or path. Focal's typed fields are in [objects.rs](../../crates/focal-model/src/objects.rs#L146); Sylk's distinct `Reference` field is in [types.go](../../../sylk/core/claims/types.go#L897).

The sources do not define external-source normalization, uniqueness, indexing, or authorization. Those remain design work. In particular, Sylk's carried-forward evidence source indexes and Hecate's forest intake deduplication discussion are not a universal ledger object lookup contract. No `GetBySource` symbol or equivalent generic claims query was found in the inspected claims packages/specs.

### 5.2 List semantics to implement consistently

- No optional filter is mandatory. `list claims`, `list testaments`, `list artifacts`, and `list validations` each return a bounded first page of authorized records.
- Repeated values for one filter form OR; different filter dimensions form AND. Set this in the typed query schema and help output. Invalid values or combinations are errors, never ignored filters.
- `--limit` is positive and server-capped. Zero must not mean unbounded. An explicit streaming export may page repeatedly with cancellation/backpressure; it is not an unbounded server allocation.
- A continuation binds ledger, query/filter hash, ordering and snapshot/read prefix, with expiry/retention semantics. Changing filters while reusing a cursor is invalid. Use `(ObjectKind, ObjectId)` where families share a scan; IDs are not assumed globally unique across all object maps.
- Perform authorization before exposing source/object existence. Filtered results, counts, page metadata and errors must not reveal another tenant's objects.
- Default ordering must be deterministic and documented. Do not promise chronological sorting merely because Sylk returns submission order for one specific query.
- A filtered scan must bound examined items/bytes as well as returned items. A page may contain zero matches and a continuation. Do not implement “return 100 matches” by scanning an arbitrarily large shard under one request.
- Mutating data across pages requires an explicit consistency choice: pinned exact prefix when available, at-least token, or clearly labeled stale projection. Expired retention returns a typed continuation/resync/expired result, not silently incomplete history.

The current wire only has `Objects`, `Scan { after }`, and `Traverse { roots, depth }`, with `max_items`; these richer predicates/cursor contracts require append-compatible protocol work. Implementing them solely by silently scanning the entire ledger in the CLI would violate the bounded graph/read intent in [LEDGER_CORE.md §3](reference/hecate/docs/specs/LEDGER_CORE.md) and [Focal ReadQuery](../../crates/focal-wire/src/message.rs#L31).

## 6. Flags, JSON/YAML, canonical identity, and results

### 6.1 One request-building path

Use a typed command builder shared by the manual CLI and complete request-file input. For example, a flags-based `submit claim` and a `submit claim --file claim.yaml` must produce equal typed authored content and equal canonical content hashes when they describe the same claim. The complete transport envelope then adds the selected ledger/route, request epoch, request identity and operation. Defaults that affect identity must be materialized and persisted before the first send.

Recommended separation:

1. **Friendly document:** versioned claim/artifact/closure specification with human-readable IDs, durations and typed relations. Resolve it under explicit authenticated context into existing model types; do not deserialize it as `AuthorityContext`.
2. **Complete request document:** exact `RequestEnvelope` for scripted automation and unknown-outcome retry. Preserve the existing `request FILE` behavior; add explicit input-format support without reinterpreting old files.
3. **Wire encoding:** use the current protocol encoder. JSON/YAML syntax, whitespace, map presentation and file extension are not wire identity. Focal's content hashes additionally use its explicit canonical authored-field encoding, distinct from its Postcard transport framing.

Sources: [RequestEnvelope](../../crates/focal-wire/src/message.rs#L171), [canonical.rs](../../crates/focal-model/src/canonical.rs#L97), and [frame.rs](../../crates/focal-wire/src/frame.rs#L34). Current `request FILE` is JSON-only and reads at most 1 MiB: [main.rs request arm](../../crates/focal-node/src/main.rs#L198). Current YAML support is deployment configuration, not evidence that wire-request YAML is already supported: [Settings::from_yaml](../../crates/focal-node/src/config.rs#L88).

Support exactly one authored-input mode: field flags, `--json DOCUMENT`, `--yaml DOCUMENT`, or `--file PATH|-` with explicit `--input-format json|yaml` where needed. Reject conflicting modes. Transport/output flags may accompany each form; there is no implicit authored-content merge. Strictly reject unknown fields, duplicate keys, multiple documents, trailing bytes, excessive nesting/aliases/lengths, invalid enum values and conflicting IDs. YAML tags or aliases must not become executable object construction or bypass byte/depth limits. Output can support `--format table|json|yaml` and bounded JSON Lines streams, with table output remaining a presentation of the same typed result. Authored `--json/--yaml` input is distinct from `--format` output.

Convenience ID formats should be lossless and unambiguous. Convert a human ID to its fixed-width Rust type once; roundtrip examples must show the accepted syntax. Do not make scripts construct raw byte arrays just because Serde currently serializes them that way, and do not silently reinterpret strings as hashes.

### 6.2 Retry, acknowledgment, and error contract

- Persist the complete intent and newly generated IDs before first submission. On timeout, disconnect, cancellation, leader change, or partial local output, retain them. “Retry” means the same operation identity and authored payload, not a newly generated claim/testament/upload.
- Only a durable committed receipt is mutation success. `Pending` and outcome unknown must be distinct machine results; read back using an authoritative retained receipt or retry the exact request. Transport acceptance and leadership-transfer initiation are not commits.
- A schema check/build is read-only and claims no future admission guarantee. A server preview, if added, reports the checked prefix and can become stale immediately; it does not reserve a claim or start validation.
- Preserve typed distinctions: structural/authentication error, committed domain outcome, capacity/yield, stale route/revision/fence, unavailable/unknown, expired retry history, and resync/retention expiry. Avoid matching error text to decide retryability.
- Exact retries after unknown control admission cannot be abandoned merely because a later server reports `Unauthorized` or `Rejected`: those can precede receipt lookup at ingress or arise at a stale owner. Journal replacement requires authoritative committed evidence that the old intent cannot later apply, or a dedicated committed cancellation/fence protocol. A bare absent receipt at one prefix is insufficient for a still-admissible pending mutation.
- A watched delta is acknowledged only after the configured sink has accepted it under the documented durability policy. Broken stdout must cause a fallible exit without advancing the saved cursor past undelivered data. Keep consumer generation, scope, delta offset and resolved markers; do not flatten the cursor to one sequence integer.
- Redact credentials and invitation tokens in every format and error path. Request files may contain sensitive authored content; never dump them automatically on failure. Displaying a principal does not confer that principal's authority.

The committed/pending distinction is explicit in [MutationReply](../../crates/focal-wire/src/message.rs#L198); stream acknowledgment is explicit in [StreamRequest](../../crates/focal-wire/src/message.rs#L222). Control retries are evaluated in [ControlReplica::submit](../../crates/focal-control/src/replica.rs), with receipt storage in [retry.rs](../../crates/focal-control/src/retry.rs); ingress authorization and asynchronous completion live in [control_host.rs](../../crates/focal-node/src/control_host.rs). Invitation identity preservation and secret output behavior are already documented in [network-startup.md](../network-startup.md#restart-and-retry).

Recommend stable exit categories with machine-readable error codes: success/complete read, usage/schema error, authentication/authorization failure, definite semantic rejection/conflict, retryable capacity/unavailability, unknown mutation outcome, and expired history/resync. Assign numeric values once and freeze them in tests. Do not make an empty authorized list an error or an incomplete validation a CLI crash. A command may succeed at recording a failing verdict; the result should distinguish “recorded” from the verdict's quality outcome.

## 7. Implementation order and acceptance evidence

This research does not add code. The actionable delivery order is:

| Slice | Work required before claiming coverage | Acceptance evidence |
|---|---|---|
| CLI-1: typed inputs and results | Shared builder, complete request-document compatibility, structured outputs/errors, bounded readers/writers, stable local request journal | Equivalent flags/JSON/YAML produce identical typed intent and hashes; malformed/oversize/duplicate inputs fail before send; secret redaction and broken-output handling |
| CLI-2: schemas and vocabulary | Exact object/command schema projection and payload registry contract; stable enum/type metadata; schema/version negotiation | Schema examples roundtrip through actual decoder; impossible/unknown fields rejected; no manually maintained schema that silently drifts from Rust types |
| CLI-3: bounded reads | Append-compatible family/source/filter query; predicate/index semantics; examined-byte limits; query-bound continuation | Unfiltered list works for all four families; optional filters compose; zero-match pages can continue; same ID in different families is unambiguous; cross-tenant negatives |
| CLI-4: actor lifecycle | Named create/post/receive/progress/evidence attach/close/cancel/supersede/wait commands | Complete manual claim → receipt → evidence → testament → validation → satisfied flow without `demo` or handwritten full wire envelopes; Generated/Post distinction and failure evidence remain visible |
| CLI-5: validator/operator surfaces | Pinned contract introspection; restricted evaluator/runtime verbs; no arbitrary actor status writes | Programmatic failure prevents quality phase; offline evaluation does not write verdict; forged principal/runtime/evidence claims rejected; missing version/schema is typed |
| CLI-6: content and watches | Resumable bounded upload/download, payload verification, durable cursor sink/journal | Interrupted chunk transfer and lost seal reply resume exact identity; consumer restart has no skipped acknowledged data; below-retention produces resync |
| CLI-7: cluster/deployment manual | Existing setup help plus bounded status; planned membership/policy workflows only as their durable services become available | Laptop commands stay unchanged after networking; invitation replay/restart; no false voter/durability claim; plan/apply rejects stale or cross-cluster plans; unsupported geography remains unknown |

Every advertised command must have an executable test against the actual service or an explicit read-only offline implementation. Help-only commands and fallback success messages do not count. The final command inventory should mechanically cover the accepted actor commands and enumerate restricted commands deliberately; adding a new `Command` or read query must fail a coverage check until its CLI disposition is documented.

For the first manual workflow test, use a fresh local data directory, create a claim with a pinned receipt/test validation, post it, acquire a receipt, begin an evidence set, upload or inline a schema-valid test report, attach the artifact, close the testament, and observe the real runtime verdict and final lifecycle. Then stop/restart and repeat the saved request identities, compare immutable IDs/hashes and committed receipts, list all four families without filters, query each by ID and supported issuer/provenance selectors, and exercise a failing/incomplete test report. This demonstrates the requested manual interface without claiming multi-region qualification.

Remaining deliberate decisions are detailed spellings for restricted operator verbs, stable numeric exit codes, the schema-catalogue distribution format, and external `SourceRef` normalization/uniqueness. The source evidence supports the operation boundaries above; [13](13-cli-and-agent-implementation-plan.md) chooses the public `submit/get/list` grammar and issuer/subject aliases. The existing implementation status remains authoritative in [09](09-implementation-status.md); proposed matrix rows are not shipped commands.
