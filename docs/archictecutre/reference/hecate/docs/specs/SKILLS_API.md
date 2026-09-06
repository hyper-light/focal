# SPEC: the skill API — one definition, five artifacts

Status: ACCEPTED 2026-08-16 (amended under maximal audit + user correction:
**TS/Python are authoring-time bindings, never runtimes** — no interpreter
executes in any microVM; declared skills are documents + compositions).
Direction ratified: typed Rust skills publishing over MCP (tools + `skill://`
per SEP-2640); compiled built-ins; Guardian-validated schema-first TS/Python
*declarations*; small per-agent surfaces. References: `SKILLS.md`
(architecture), `PROTOCOL.md` §6 (hecate-wire), `REGISTRY.md` (Skill kind,
staging, `DocValue`).

## 1. The Rust surface

```rust
#[derive(Skill)]                       // the single interpreter of the definition
#[skill(name = "workspace", domain = Domain::Implementation)]
pub struct WorkspaceSkill { /* handles into owning stores — no Arc, per doctrine */ }

impl SkillImpl for WorkspaceSkill {
    type Action = WorkspaceAction;     // #[derive(Wire)] closed enum — the façade
    type Output = WorkspaceOutput;     // #[derive(Wire)]
    const INSTRUCTIONS: &'static str = include_str!("workspace.skill.md");
    const CONTRACT: Contract = Contract {
        produces: &[ArtifactKind::FileChange],
        consumes: &[ArtifactKind::WriteBasis],
    };
    const CAPABILITIES: Capabilities = Capabilities::WORKSPACE_RW; // Guardian-visible
    async fn invoke(&mut self, cx: &mut SkillCx, a: Self::Action)
        -> Result<Self::Output, SkillError>;
}
```

The derive emits, from this one definition: (1) the hecate-wire codecs and schema
hashes; (2) the MCP tool projection (JSON Schema generated from the same reflection
— no serde, no schemars, no second interpreter); (3) the `skill://` instructional
resource (INSTRUCTIONS + supporting files, content-digested); (4) the registry
`Skill` document (name, domain, contract, **declared capabilities**); (5) dispatch
glue with input/output validation at the boundary.

- **Façade-first**: one skill, one closed `Action` enum — never sibling verb
  families. Append-only evolution checked against the committed schema snapshot.
- `SkillCx` carries the claims façade, the store handles, and the emitters — skills
  reach the world through it, never ambiently.
- **Statelessness (amendment)**: built-in skill structs hold only store handles
  and config — no work-bearing fields between invocations (replay and
  brief+claims handoff safety) — enforced by the runtime's lint-wall pattern;
  anything a skill wants remembered goes through `SkillCx` into an owning
  store. (Declared skills cannot hold state by construction — they are
  documents.)
- **Capabilities are a closed, harness-versioned bitset (amendment)**: every
  capability carries a `compile_to_warden()` mapping to policy atoms — a
  capability without warden enforcement semantics cannot exist by
  construction; unknown names in declarations reject at staging; new
  capabilities ship only with a harness release.

## 2. TS/Python bindings (authoring SDKs, never runtimes)

**The law**: TS/Python are friendly, typed authoring surfaces for defining
Hecate extensions — skills here, and every other registry kind through the same
generator (agents/AgentRole, prompts, recipes, MCP server declarations). Their
output is always a **canonical document**; only documents ever cross into
Hecate. **No Node, no Python, no interpreter executes in any microVM.**

- **Pipeline**: author writes the definition against the generated SDK (typed,
  IDE-friendly, generated from the same schema reflection that serves MCP and
  the registry — kind descriptors are the generation source, so
  config-registered kinds get SDKs too) → the binding tool validates locally
  and emits the **canonical `DocValue` document** → apply → Guardian staging
  (inventory-from-declaration, content-bound approval) → bundle → delivered to
  pods at assignment as a document over the control channel (`REGISTRY.md`
  §4b). Authoring convenience on the outside; one canonical form inside.
- **Invocation is composition, not code.** A declared skill contributes its
  `skill://` instructional resource (always) and optional tool projections
  whose dispatch targets are a closed set of two:

  ```rust
  enum DispatchTarget {
      Facade { target: FacadeRef, map: ActionTemplate },   // existing typed harness façade
      ToolExec { recipe: RecipeRef, args: ArgTemplate },   // provisioned CLI via the tool plane
  }
  ```

  Templates are **pure field mappings** — schema-validated parameter
  projection, literals allowed, logic forbidden (logic belongs in the agent's
  judgment or in a provisioned tool binary). Staging validates
  template↔schema compatibility statically — declared skills are data +
  mappings, so verification is total; the compiled-Rust dispatcher executes
  the mapping; the warden gates the effect exactly as it would the same
  façade/exec reached any other way.
- **Arbitrary custom behavior enters Hecate exactly one way**: ship a
  binary/CLI through the provisioner (Recipe → content-addressed tool
  manifest → RO tool mount) and declare a skill whose actions `ToolExec` it —
  a normal warden-gated process in the pod. The tool's language is the tool
  plane's business, not the skill system's.
- **Capabilities are derived, not trusted**: a declared skill's capability set
  is computed at staging from its dispatch targets — over- or
  under-declaration is a typed staging error (S9).

## 3. Surface discipline

- An agent's default surface = the shared façades + its role façades — a
  derived handful (counted, tested), with progressive disclosure (`search` +
  activation) behind it. Activation state can shrink.
- **Omission is absence**: a capability a role must never hold is not registered in
  its bundle — invoking it is *unknown tool*, not *denied tool*.

## 4. Tests and acceptance

| # | Test | Catches |
|---|---|---|
| S1 | Schema fidelity: Rust type → JSON Schema → TS/Py SDK round-trip against the committed vector corpus | drift between wire, MCP, and SDKs |
| S2 | Omission semantics: unregistered skill ⇒ unknown (not denied); registered-but-inactive ⇒ discoverable via search | gating theater; hidden-but-present leaks |
| S3 | Contract enforcement: produces/consumes checked at dispatch; violations typed | protocol artifacts drifting from declarations |
| S4 | Capability ⊆ bundle: staging refuses any declaration exceeding its bundle's grants; warden enforcement holds even if staging were bypassed | privilege via declaration |
| S5 | Evolution trybuild: field/variant removal or reorder in any skill type is a compile error against the snapshot | silent wire breaks in tool schemas |
| S6 | Surface count: each role's default-visible surface within its derived budget (CI-counted; the Sylk 31-tool turn is the named regression) | tool-surface sprawl returning |
| S7 | Binding fidelity: the same logical declaration authored via TS, via Python, or as a direct document ⇒ byte-identical canonical form and hash | binding dialects forking identity |
| S8 | No-execution structural: no code path executes declared-skill content — the only executable skill code in the tree is compiled Rust; declared skills are data + mappings (architecture test + lint) | an interpreter creeping into pods |
| S9 | Capability derivation: declared capability set ≠ set implied by dispatch targets ⇒ typed staging error; unknown capability name ⇒ rejection; every capability has a warden compilation (architecture test) | privilege via declaration; unenforceable capabilities |

Acceptance: one derive produces all five artifacts (no second interpreter of the
Rust definition exists); bindings emit canonical documents only — no declared
code ever executes (S8 permanent); Guardian static validation precedes every
declared entry's availability, structurally; S1/S5/S6/S8 permanent; `SkillCx`
is the only capability path (no ambient reaches — lint); built-in skills are
stateless between invocations (lint).
